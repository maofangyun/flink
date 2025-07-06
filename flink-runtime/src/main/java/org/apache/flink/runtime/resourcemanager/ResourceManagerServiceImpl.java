/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link ResourceManagerService}. */
public class ResourceManagerServiceImpl implements ResourceManagerService, LeaderContender {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerServiceImpl.class);

    private final ResourceManagerFactory<?> resourceManagerFactory;
    private final ResourceManagerProcessContext rmProcessContext;

    private final LeaderElection leaderElection;

    private final FatalErrorHandler fatalErrorHandler;
    private final Executor ioExecutor;

    private final ExecutorService handleLeaderEventExecutor;
    private final CompletableFuture<Void> serviceTerminationFuture;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private boolean running;

    @Nullable
    @GuardedBy("lock")
    private ResourceManager<?> leaderResourceManager;

    @Nullable
    @GuardedBy("lock")
    private UUID leaderSessionID;

    @GuardedBy("lock")
    private CompletableFuture<Void> previousResourceManagerTerminationFuture;

    private ResourceManagerServiceImpl(
            ResourceManagerFactory<?> resourceManagerFactory,
            ResourceManagerProcessContext rmProcessContext)
            throws Exception {
        this.resourceManagerFactory = checkNotNull(resourceManagerFactory);
        this.rmProcessContext = checkNotNull(rmProcessContext);

        this.leaderElection =
                rmProcessContext.getHighAvailabilityServices().getResourceManagerLeaderElection();
        this.fatalErrorHandler = rmProcessContext.getFatalErrorHandler();
        this.ioExecutor = rmProcessContext.getIoExecutor();

        this.handleLeaderEventExecutor = Executors.newSingleThreadExecutor();
        this.serviceTerminationFuture = new CompletableFuture<>();

        this.running = false;
        this.leaderResourceManager = null;
        this.leaderSessionID = null;
        this.previousResourceManagerTerminationFuture = FutureUtils.completedVoidFuture();
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerService
    // ------------------------------------------------------------------------

    /**
     * 启动资源管理器服务。若服务已经启动，则仅记录调试日志并返回；
     * 若服务未启动，则将服务状态标记为运行中，并启动领导者选举流程。
     *
     * @throws Exception 启动领导者选举过程中可能抛出的异常
     */
    @Override
    public void start() throws Exception {
        // 使用同步块确保线程安全，避免多个线程同时启动服务
        synchronized (lock) {
            // 检查服务是否已经处于运行状态
            if (running) {
                // 若服务已启动，记录调试日志并直接返回
                LOG.debug("Resource manager service has already started.");
                return;
            }
            // 将服务状态标记为运行中
            running = true;
        }

        // 记录信息日志，表明开始启动资源管理器服务
        LOG.info("Starting resource manager service.");

        // 调用领导者选举服务的 startLeaderElection 方法，开始领导者选举流程
        // 传入 this 表示当前类实例作为领导者竞争者参与选举
        leaderElection.startLeaderElection(this);
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return serviceTerminationFuture;
    }

    @Override
    public CompletableFuture<Void> deregisterApplication(
            final ApplicationStatus applicationStatus, final @Nullable String diagnostics) {

        synchronized (lock) {
            if (!running || leaderResourceManager == null) {
                return deregisterWithoutLeaderRm();
            }

            final ResourceManager<?> currentLeaderRM = leaderResourceManager;
            return currentLeaderRM
                    .getStartedFuture()
                    .thenCompose(
                            ignore -> {
                                synchronized (lock) {
                                    if (isLeader(currentLeaderRM)) {
                                        return currentLeaderRM
                                                .getSelfGateway(ResourceManagerGateway.class)
                                                .deregisterApplication(
                                                        applicationStatus, diagnostics)
                                                .thenApply(ack -> null);
                                    } else {
                                        return deregisterWithoutLeaderRm();
                                    }
                                }
                            });
        }
    }

    private static CompletableFuture<Void> deregisterWithoutLeaderRm() {
        LOG.warn("Cannot deregister application. Resource manager service is not available.");
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (running) {
                LOG.info("Stopping resource manager service.");
                running = false;
                stopLeaderElectionService();
                stopLeaderResourceManager();
            } else {
                LOG.debug("Resource manager service is not running.");
            }

            FutureUtils.forward(previousResourceManagerTerminationFuture, serviceTerminationFuture);
        }

        handleLeaderEventExecutor.shutdownNow();

        return serviceTerminationFuture;
    }

    // ------------------------------------------------------------------------
    //  LeaderContender
    // ------------------------------------------------------------------------

    @Override
    public void grantLeadership(UUID newLeaderSessionID) {
        handleLeaderEventExecutor.execute(
                () -> {
                    synchronized (lock) {
                        if (!running) {
                            LOG.info(
                                    "Resource manager service is not running. Ignore granting leadership with session ID {}.",
                                    newLeaderSessionID);
                            return;
                        }

                        LOG.info(
                                "Resource manager service is granted leadership with session id {}.",
                                newLeaderSessionID);

                        try {
                            startNewLeaderResourceManager(newLeaderSessionID);
                        } catch (Throwable t) {
                            fatalErrorHandler.onFatalError(
                                    new FlinkException("Cannot start resource manager.", t));
                        }
                    }
                });
    }

    @Override
    public void revokeLeadership() {
        handleLeaderEventExecutor.execute(
                () -> {
                    synchronized (lock) {
                        if (!running) {
                            LOG.info(
                                    "Resource manager service is not running. Ignore revoking leadership.");
                            return;
                        }

                        LOG.info(
                                "Resource manager service is revoked leadership with session id {}.",
                                leaderSessionID);

                        stopLeaderResourceManager();

                        if (!resourceManagerFactory.supportMultiLeaderSession()) {
                            closeAsync();
                        }
                    }
                });
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(
                new FlinkException(
                        "Exception during leader election of resource manager occurred.",
                        exception));
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    @GuardedBy("lock")
    private void startNewLeaderResourceManager(UUID newLeaderSessionID) throws Exception {
        stopLeaderResourceManager();

        this.leaderSessionID = newLeaderSessionID;
        this.leaderResourceManager =
                resourceManagerFactory.createResourceManager(rmProcessContext, newLeaderSessionID);

        final ResourceManager<?> newLeaderResourceManager = this.leaderResourceManager;

        previousResourceManagerTerminationFuture
                .thenComposeAsync(
                        (ignore) -> {
                            synchronized (lock) {
                                return startResourceManagerIfIsLeader(newLeaderResourceManager);
                            }
                        },
                        handleLeaderEventExecutor)
                .thenAcceptAsync(
                        (isStillLeader) -> {
                            if (isStillLeader) {
                                leaderElection.confirmLeadershipAsync(
                                        newLeaderSessionID, newLeaderResourceManager.getAddress());
                            }
                        },
                        ioExecutor);
    }

    /**
     * Returns a future that completes as {@code true} if the resource manager is still leader and
     * started, and {@code false} if it's no longer leader.
     */
    @GuardedBy("lock")
    private CompletableFuture<Boolean> startResourceManagerIfIsLeader(
            ResourceManager<?> resourceManager) {
        if (isLeader(resourceManager)) {
            resourceManager.start();
            forwardTerminationFuture(resourceManager);
            return resourceManager.getStartedFuture().thenApply(ignore -> true);
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    private void forwardTerminationFuture(ResourceManager<?> resourceManager) {
        resourceManager
                .getTerminationFuture()
                .whenComplete(
                        (ignore, throwable) -> {
                            synchronized (lock) {
                                if (isLeader(resourceManager)) {
                                    if (throwable != null) {
                                        serviceTerminationFuture.completeExceptionally(throwable);
                                    } else {
                                        serviceTerminationFuture.complete(null);
                                    }
                                }
                            }
                        });
    }

    @GuardedBy("lock")
    private boolean isLeader(ResourceManager<?> resourceManager) {
        return running && this.leaderResourceManager == resourceManager;
    }

    @GuardedBy("lock")
    private void stopLeaderResourceManager() {
        if (leaderResourceManager != null) {
            previousResourceManagerTerminationFuture =
                    previousResourceManagerTerminationFuture.thenCombine(
                            leaderResourceManager.closeAsync(), (ignore1, ignore2) -> null);
            leaderResourceManager = null;
            leaderSessionID = null;
        }
    }

    private void stopLeaderElectionService() {
        try {
            if (leaderElection != null) {
                leaderElection.close();
            }
        } catch (Exception e) {
            serviceTerminationFuture.completeExceptionally(
                    new FlinkException("Cannot stop leader election service.", e));
        }
    }

    @VisibleForTesting
    @Nullable
    public ResourceManager<?> getLeaderResourceManager() {
        synchronized (lock) {
            return leaderResourceManager;
        }
    }

    public static ResourceManagerServiceImpl create(
            ResourceManagerFactory<?> resourceManagerFactory,
            Configuration configuration,
            ResourceID resourceId,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            MetricRegistry metricRegistry,
            String hostname,
            Executor ioExecutor)
            throws Exception {

        return new ResourceManagerServiceImpl(
                resourceManagerFactory,
                resourceManagerFactory.createResourceManagerProcessContext(
                        configuration,
                        resourceId,
                        rpcService,
                        highAvailabilityServices,
                        heartbeatServices,
                        delegationTokenManager,
                        fatalErrorHandler,
                        clusterInformation,
                        webInterfaceUrl,
                        metricRegistry,
                        hostname,
                        ioExecutor));
    }
}
