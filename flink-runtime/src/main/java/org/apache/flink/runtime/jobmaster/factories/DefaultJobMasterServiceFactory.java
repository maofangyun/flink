/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blocklist.BlocklistUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTrackerImpl;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentReconciler;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterService;
import org.apache.flink.runtime.jobmaster.SlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.function.FunctionUtils;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@link JobMasterServiceFactory} 的默认实现。
 * 该工厂创建 {@link JobMaster} 实例作为 {@link JobMasterService}。
 * 它负责组装 JobMaster 所需的所有依赖项和服务，例如 RPC 服务、高可用性服务和共享的 JobManager 服务。
 */
public class DefaultJobMasterServiceFactory implements JobMasterServiceFactory {

    private final Executor executor;
    private final RpcService rpcService;
    private final JobMasterConfiguration jobMasterConfiguration;
    private final ExecutionPlan executionPlan;
    private final HighAvailabilityServices haServices;
    private final SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory;
    private final JobManagerSharedServices jobManagerSharedServices;
    private final HeartbeatServices heartbeatServices;
    private final JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory;
    private final FatalErrorHandler fatalErrorHandler;
    private final ClassLoader userCodeClassloader;
    private final ShuffleMaster<?> shuffleMaster;
    private final Collection<FailureEnricher> failureEnrichers;
    private final long initializationTimestamp;

    /**
     * 构造一个新的 {@code DefaultJobMasterServiceFactory}。
     *
     * @param executor 用于运行异步操作（如创建 JobMaster）的执行器。
     * @param rpcService 用于通信的 RPC 服务。
     * @param jobMasterConfiguration JobMaster 的配置。
     * @param executionPlan 要运行的作业的执行计划。
     * @param haServices 用于领导者选举和元数据持久化的高可用性服务。
     * @param slotPoolServiceSchedulerFactory 用于创建管理 slot 的调度器的工厂。
     * @param jobManagerSharedServices 在 JobManager 级别可用的共享服务。
     * @param heartbeatServices 用于监控 TaskManager 心跳的服务。
     * @param jobManagerJobMetricGroupFactory 用于为作业创建度量组的工厂。
     * @param fatalErrorHandler 可能发生的致命错误的处理程序。
     * @param userCodeClassloader 用户作业代码的类加载器。
     * @param failureEnrichers 用于为故障添加更多上下文的丰富器集合。
     * @param initializationTimestamp 作业初始化时的时间戳��
     */
    public DefaultJobMasterServiceFactory(
            Executor executor,
            RpcService rpcService,
            JobMasterConfiguration jobMasterConfiguration,
            ExecutionPlan executionPlan,
            HighAvailabilityServices haServices,
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            JobManagerSharedServices jobManagerSharedServices,
            HeartbeatServices heartbeatServices,
            JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
            FatalErrorHandler fatalErrorHandler,
            ClassLoader userCodeClassloader,
            Collection<FailureEnricher> failureEnrichers,
            long initializationTimestamp) {
        this.executor = executor;
        this.rpcService = rpcService;
        this.jobMasterConfiguration = jobMasterConfiguration;
        this.executionPlan = executionPlan;
        this.haServices = haServices;
        this.slotPoolServiceSchedulerFactory = slotPoolServiceSchedulerFactory;
        this.jobManagerSharedServices = jobManagerSharedServices;
        this.heartbeatServices = heartbeatServices;
        this.jobManagerJobMetricGroupFactory = jobManagerJobMetricGroupFactory;
        this.fatalErrorHandler = fatalErrorHandler;
        this.userCodeClassloader = userCodeClassloader;
        this.shuffleMaster = jobManagerSharedServices.getShuffleMaster();
        this.failureEnrichers = failureEnrichers;
        this.initializationTimestamp = initializationTimestamp;
    }

    /**
     * 异步创建一个新的 {@link JobMasterService} 实例。
     * 此方法将实际的实例化委托给一个内部方法，并将调用包装在 {@link CompletableFuture} 中，以便在提供的执行器上执行。
     *
     * @param leaderSessionId 新领导者会话的 UUID。这标识了 JobMaster 作为领导者的特定实例。
     * @param onCompletionActions 作业完成时要执行的操作（例如，关闭集群）。
     * @return 一个 {@link CompletableFuture}，完成后将持有创建的 {@link JobMasterService}。
     */
    @Override
    public CompletableFuture<JobMasterService> createJobMasterService(
            UUID leaderSessionId, OnCompletionActions onCompletionActions) {

        // 在指定的执行器上异步执行任务，创建 JobMasterService 实例
        return CompletableFuture.supplyAsync(
                // 使用 FunctionUtils.uncheckedSupplier 包装创建逻辑，将受检异常转换为运行时异常
                FunctionUtils.uncheckedSupplier(
                        () -> internalCreateJobMasterService(leaderSessionId, onCompletionActions)),
                // 指定执行异步任务的执行器
                executor);
    }

    /**
     * 执行 {@link JobMaster} 的实际创建和启动。
     * 此方法由 {@link #createJobMasterService(UUID, OnCompletionActions)} 内部调用。
     * 它使用其所有依赖项实例化 {@link JobMaster}，然后调用其 {@code start()} 方法以开始其操作。
     *
     * @param leaderSessionId 新领导者会话的 UUID。
     * @param onCompletionActions 作业完成时要执行的操作。
     * @return 完全初始化并启动的 {@link JobMasterService}。
     * @throws Exception 如果在 JobMaster 的实例化或启动过程中发生任何错误。
     */
    private JobMasterService internalCreateJobMasterService(
            UUID leaderSessionId, OnCompletionActions onCompletionActions) throws Exception {
        // 创建JobMaster的地方，标记一下
        final JobMaster jobMaster =
                new JobMaster(
                        rpcService,
                        JobMasterId.fromUuidOrNull(leaderSessionId),
                        jobMasterConfiguration,
                        ResourceID.generate(),
                        executionPlan,
                        haServices,
                        slotPoolServiceSchedulerFactory,
                        jobManagerSharedServices,
                        heartbeatServices,
                        jobManagerJobMetricGroupFactory,
                        onCompletionActions,
                        fatalErrorHandler,
                        userCodeClassloader,
                        shuffleMaster,
                        lookup ->
                                new JobMasterPartitionTrackerImpl(
                                        executionPlan.getJobID(), shuffleMaster, lookup),
                        new DefaultExecutionDeploymentTracker(),
                        DefaultExecutionDeploymentReconciler::new,
                        BlocklistUtils.loadBlocklistHandlerFactory(
                                jobMasterConfiguration.getConfiguration()),
                        failureEnrichers,
                        initializationTimestamp);
        // 启动实例，会调用到JobMaster的onStart()方法
        jobMaster.start();

        return jobMaster;
    }
}
