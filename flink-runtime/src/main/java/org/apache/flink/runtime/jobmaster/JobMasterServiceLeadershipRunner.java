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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.dispatcher.JobCancellationFailedException;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * Leadership runner for the {@link JobMasterServiceProcess}.
 *
 * <p>The responsibility of this component is to manage the leadership of the {@link
 * JobMasterServiceProcess}. This means that the runner will create an instance of the process when
 * it obtains the leadership. The process is stopped once the leadership is revoked.
 *
 * <p>This component only accepts signals (job result completion, initialization failure) as long as
 * it is running and as long as the signals are coming from the current leader process. This ensures
 * that only the current leader can affect this component.
 *
 * <p>All leadership operations are serialized. This means that granting the leadership has to
 * complete before the leadership can be revoked and vice versa.
 *
 * <p>The {@link #resultFuture} can be completed with the following values: * *
 *
 * <ul>
 *   <li>{@link JobManagerRunnerResult} to signal an initialization failure of the {@link
 *       JobMasterService} or the completion of a job
 *   <li>{@link Exception} to signal an unexpected failure
 * </ul>
 */
public class JobMasterServiceLeadershipRunner implements JobManagerRunner, LeaderContender {

    private static final Logger LOG =
            LoggerFactory.getLogger(JobMasterServiceLeadershipRunner.class);

    private final Object lock = new Object();

    private final JobMasterServiceProcessFactory jobMasterServiceProcessFactory;

    private final LeaderElection leaderElection;

    private final JobResultStore jobResultStore;

    private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

    private final FatalErrorHandler fatalErrorHandler;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    @GuardedBy("lock")
    private State state = State.RUNNING;

    @GuardedBy("lock")
    private CompletableFuture<Void> sequentialOperation = FutureUtils.completedVoidFuture();

    @GuardedBy("lock")
    private JobMasterServiceProcess jobMasterServiceProcess =
            JobMasterServiceProcess.waitingForLeadership();

    @GuardedBy("lock")
    private CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private boolean hasCurrentLeaderBeenCancelled = false;

    public JobMasterServiceLeadershipRunner(
            JobMasterServiceProcessFactory jobMasterServiceProcessFactory,
            LeaderElection leaderElection,
            JobResultStore jobResultStore,
            LibraryCacheManager.ClassLoaderLease classLoaderLease,
            FatalErrorHandler fatalErrorHandler) {
        this.jobMasterServiceProcessFactory = jobMasterServiceProcessFactory;
        this.leaderElection = leaderElection;
        this.jobResultStore = jobResultStore;
        this.classLoaderLease = classLoaderLease;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> processTerminationFuture;
        synchronized (lock) {
            if (state == State.STOPPED) {
                return terminationFuture;
            }

            state = State.STOPPED;

            LOG.debug("Terminating the leadership runner for job {}.", getJobID());

            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "JobMasterServiceLeadershipRunner is closed. Therefore, the corresponding JobMaster will never acquire the leadership."));

            resultFuture.complete(
                    JobManagerRunnerResult.forSuccess(
                            createExecutionGraphInfoWithJobStatus(JobStatus.SUSPENDED)));

            processTerminationFuture = jobMasterServiceProcess.closeAsync();
        }

        final CompletableFuture<Void> serviceTerminationFuture =
                FutureUtils.runAfterwards(
                        processTerminationFuture,
                        () -> {
                            classLoaderLease.release();
                            leaderElection.close();
                        });

        FutureUtils.forward(serviceTerminationFuture, terminationFuture);

        terminationFuture.whenComplete(
                (unused, throwable) ->
                        LOG.debug("Leadership runner for job {} has been terminated.", getJobID()));
        return terminationFuture;
    }

    /**
     * 启动领导选举过程，使当前实例参与到领导竞争中。
     * 此方法会触发领导选举服务开始工作，当当前实例获取领导权时，
     * 会创建并启动 JobMasterServiceProcess 来管理作业。
     *
     * @throws Exception 若在启动领导选举服务过程中出现异常
     */
    @Override
    public void start() throws Exception {
        // 记录启动领导选举运行器的调试日志，包含作业的 ID
        LOG.debug("Start leadership runner for job {}.", getJobID());
        // 调用领导选举服务的方法，开始领导选举过程，并将当前实例作为竞争候选者传入
        leaderElection.startLeaderElection(this);
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        synchronized (lock) {
            return jobMasterGatewayFuture;
        }
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public JobID getJobID() {
        return jobMasterServiceProcessFactory.getJobId();
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(Duration timeout) {
        synchronized (lock) {
            hasCurrentLeaderBeenCancelled = true;
            return getJobMasterGateway()
                    .thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout))
                    .exceptionally(
                            e -> {
                                throw new CompletionException(
                                        new JobCancellationFailedException(
                                                "Cancellation failed.",
                                                ExceptionUtils.stripCompletionException(e)));
                            });
        }
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Duration timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                executionGraphInfo.getArchivedExecutionGraph().getState());
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Duration timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                JobDetails.createDetailsForJob(
                                        executionGraphInfo.getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Duration timeout) {
        synchronized (lock) {
            if (state == State.RUNNING) {
                if (jobMasterServiceProcess.isInitializedAndRunning()) {
                    return getJobMasterGateway()
                            .thenCompose(jobMasterGateway -> jobMasterGateway.requestJob(timeout));
                } else {
                    return CompletableFuture.completedFuture(
                            createExecutionGraphInfoWithJobStatus(
                                    hasCurrentLeaderBeenCancelled
                                            ? JobStatus.CANCELLING
                                            : JobStatus.INITIALIZING));
                }
            } else {
                return resultFuture.thenApply(JobManagerRunnerResult::getExecutionGraphInfo);
            }
        }
    }

    @Override
    public boolean isInitialized() {
        synchronized (lock) {
            return jobMasterServiceProcess.isInitializedAndRunning();
        }
    }

    /**
     * 当当前实例被授予领导权时调用此方法。
     * 若领导选举运行器处于运行状态，将异步启动一个新的 JobMasterServiceProcess 来管理作业。
     *
     * @param leaderSessionID 领导会话的唯一标识符，用于标识本次领导权的会话
     */
    @Override
    public void grantLeadership(UUID leaderSessionID) {
        // 检查领导选举运行器是否处于运行状态，若是则执行传入的操作
        // 这里的操作是异步启动一个新的 JobMasterServiceProcess
        runIfStateRunning(
                () -> startJobMasterServiceProcessAsync(leaderSessionID),
                "starting a new JobMasterServiceProcess");
    }


    /**
     * 异步启动 JobMasterServiceProcess。该方法会先检查作业是否已有结果，
     * 根据检查结果决定是处理作业已完成的情况，还是创建新的 JobMasterServiceProcess。
     * 所有操作都会按顺序执行，确保领导操作的序列化。
     *
     * @param leaderSessionId 领导会话的唯一标识符，用于标识本次领导权的会话
     */
    @GuardedBy("lock")
    private void startJobMasterServiceProcessAsync(UUID leaderSessionId) {
        // 将当前操作添加到顺序操作链中，确保操作按顺序执行
        sequentialOperation =
                sequentialOperation.thenCompose(
                        unused ->
                                // 异步检查作业结果存储中是否已有该作业的结果
                                jobResultStore
                                        .hasJobResultEntryAsync(getJobID())
                                        .thenCompose(
                                                hasJobResult -> {
                                                    if (hasJobResult) {
                                                        // 若作业已有结果，调用 handleJobAlreadyDoneIfValidLeader 方法处理作业已完成的情况
                                                        return handleJobAlreadyDoneIfValidLeader(
                                                                leaderSessionId);
                                                    } else {
                                                        // 若作业没有结果，调用 createNewJobMasterServiceProcessIfValidLeader 方法创建新的 JobMasterServiceProcess
                                                        return createNewJobMasterServiceProcessIfValidLeader(
                                                                leaderSessionId);
                                                    }
                                                }));
        // 处理异步操作可能出现的错误，若操作失败，会记录错误信息并处理
        handleAsyncOperationError(sequentialOperation, "Could not start the job manager.");
    }

    private CompletableFuture<Void> handleJobAlreadyDoneIfValidLeader(UUID leaderSessionId) {
        return runIfValidLeader(
                leaderSessionId, () -> jobAlreadyDone(leaderSessionId), "check completed job");
    }

    /**
     * 若当前实例是有效的领导者，则异步创建一个新的 JobMasterServiceProcess 实例。
     * 该方法会先检查当前实例是否仍拥有领导权，若拥有则执行创建操作。
     *
     * @param leaderSessionId 领导会话的唯一标识符，用于标识本次领导权的会话
     * @return 一个 CompletableFuture，当操作完成时会被标记为完成状态
     */
    private CompletableFuture<Void> createNewJobMasterServiceProcessIfValidLeader(
            UUID leaderSessionId) {
        // 调用 runIfValidLeader 方法，检查当前实例是否为有效的领导者
        // 若有效，则执行创建新 JobMasterServiceProcess 的操作
        // 若无效，则记录相应日志
        return runIfValidLeader(
                leaderSessionId,
                () ->
                        // JobMasterServiceProcess 实例化的主要工作仍异步执行
                        // 参考 DefaultJobMasterServiceFactory#createJobMasterService 在
                        // DefaultLeaderElectionService 的 leaderOperation 线程上执行逻辑，因此这里是可行的
                        ThrowingRunnable.unchecked(
                                        // 调用 createNewJobMasterServiceProcess 方法创建新实例
                                        () -> createNewJobMasterServiceProcess(leaderSessionId))
                                .run(),
                "create new job master service process");
    }

    private void printLogIfNotValidLeader(String actionDescription, UUID leaderSessionId) {
        LOG.debug(
                "Ignore leader action '{}' because the leadership runner is no longer the valid leader for {}.",
                actionDescription,
                leaderSessionId);
    }

    private ExecutionGraphInfo createExecutionGraphInfoWithJobStatus(JobStatus jobStatus) {
        return new ExecutionGraphInfo(
                jobMasterServiceProcessFactory.createArchivedExecutionGraph(jobStatus, null));
    }

    private void jobAlreadyDone(UUID leaderSessionId) {
        LOG.info(
                "{} for job {} was granted leadership with leader id {}, but job was already done.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId);
        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        new ExecutionGraphInfo(
                                jobMasterServiceProcessFactory.createArchivedExecutionGraph(
                                        JobStatus.FAILED,
                                        new JobAlreadyDoneException(getJobID())))));
    }

    /**
     * 创建一个新的 JobMasterServiceProcess 实例。
     * 该方法会先确保当前的 JobMasterServiceProcess 已经关闭，
     * 然后创建一个新的实例，并进行相关的初始化操作，
     * 如转发 JobMasterGatewayFuture、结果未来以及确认领导权。
     *
     * @param leaderSessionId 领导会话的唯一标识符，用于标识本次领导权的会话
     */
    @GuardedBy("lock")
    private void createNewJobMasterServiceProcess(UUID leaderSessionId) {
        // 检查当前的 JobMasterServiceProcess 是否已经关闭
        // 若未关闭，会抛出 IllegalStateException 异常
        Preconditions.checkState(jobMasterServiceProcess.closeAsync().isDone());

        // 记录日志，表明当前实例已被授予领导权，并即将创建新的 JobMasterServiceProcess
        LOG.info(
                "{} for job {} was granted leadership with leader id {}. Creating new {}.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId,
                JobMasterServiceProcess.class.getSimpleName());

        // 使用工厂方法创建一个新的 JobMasterServiceProcess 实例
        jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId);

        // 如果当前实例仍然是有效的领导者，则将 JobMasterServiceProcess 的 JobMasterGatewayFuture 转发到本地的 jobMasterGatewayFuture
        forwardIfValidLeader(
                leaderSessionId,
                jobMasterServiceProcess.getJobMasterGatewayFuture(),
                jobMasterGatewayFuture,
                "JobMasterGatewayFuture from JobMasterServiceProcess");
        // 转发 JobMasterServiceProcess 的结果未来，当作业完成时进行相应处理
        forwardResultFuture(leaderSessionId, jobMasterServiceProcess.getResultFuture());
        // 确认领导权，将 JobMasterServiceProcess 的领导地址发送给领导者选举服务
        confirmLeadership(leaderSessionId, jobMasterServiceProcess.getLeaderAddressFuture());
    }

    private void confirmLeadership(
            UUID leaderSessionId, CompletableFuture<String> leaderAddressFuture) {
        FutureUtils.assertNoException(
                leaderAddressFuture.thenCompose(
                        address ->
                                callIfRunning(
                                                () -> {
                                                    LOG.debug(
                                                            "Confirm leadership {}.",
                                                            leaderSessionId);
                                                    return leaderElection.confirmLeadershipAsync(
                                                            leaderSessionId, address);
                                                },
                                                "confirming leadership")
                                        .orElse(FutureUtils.completedVoidFuture())));
    }

    private void forwardResultFuture(
            UUID leaderSessionId, CompletableFuture<JobManagerRunnerResult> resultFuture) {
        resultFuture.whenComplete(
                (jobManagerRunnerResult, throwable) ->
                        runIfValidLeader(
                                leaderSessionId,
                                () -> onJobCompletion(jobManagerRunnerResult, throwable),
                                "result future forwarding"));
    }

    @GuardedBy("lock")
    private void onJobCompletion(
            JobManagerRunnerResult jobManagerRunnerResult, Throwable throwable) {
        state = State.JOB_COMPLETED;

        LOG.debug("Completing the result for job {}.", getJobID());

        if (throwable != null) {
            resultFuture.completeExceptionally(throwable);
            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "Could not retrieve JobMasterGateway because the JobMaster failed.",
                            throwable));
        } else {
            if (!jobManagerRunnerResult.isSuccess()) {
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException(
                                "Could not retrieve JobMasterGateway because the JobMaster initialization failed.",
                                jobManagerRunnerResult.getInitializationFailure()));
            }

            resultFuture.complete(jobManagerRunnerResult);
        }
    }

    @Override
    public void revokeLeadership() {
        runIfStateRunning(
                this::stopJobMasterServiceProcessAsync,
                "revoke leadership from JobMasterServiceProcess");
    }

    @GuardedBy("lock")
    private void stopJobMasterServiceProcessAsync() {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        ignored ->
                                callIfRunning(
                                                this::stopJobMasterServiceProcess,
                                                "stop leading JobMasterServiceProcess")
                                        .orElse(FutureUtils.completedVoidFuture()));

        handleAsyncOperationError(sequentialOperation, "Could not suspend the job manager.");
    }

    @GuardedBy("lock")
    private CompletableFuture<Void> stopJobMasterServiceProcess() {
        LOG.info(
                "{} for job {} was revoked leadership with leader id {}. Stopping current {}.",
                getClass().getSimpleName(),
                getJobID(),
                jobMasterServiceProcess.getLeaderSessionId(),
                JobMasterServiceProcess.class.getSimpleName());

        jobMasterGatewayFuture.completeExceptionally(
                new FlinkException(
                        "Cannot obtain JobMasterGateway because the JobMaster lost leadership."));
        jobMasterGatewayFuture = new CompletableFuture<>();

        hasCurrentLeaderBeenCancelled = false;

        return jobMasterServiceProcess.closeAsync();
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(exception);
    }

    private void handleAsyncOperationError(CompletableFuture<Void> operation, String message) {
        operation.whenComplete(
                (unused, throwable) -> {
                    if (throwable != null) {
                        runIfStateRunning(
                                () ->
                                        handleJobMasterServiceLeadershipRunnerError(
                                                new FlinkException(message, throwable)),
                                "handle JobMasterServiceLeadershipRunner error");
                    }
                });
    }

    private void handleJobMasterServiceLeadershipRunnerError(Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            fatalErrorHandler.onFatalError(cause);
        } else {
            resultFuture.completeExceptionally(cause);
        }
    }

    private void runIfStateRunning(Runnable action, String actionDescription) {
        synchronized (lock) {
            if (isRunning()) {
                action.run();
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        actionDescription);
            }
        }
    }

    private <T> Optional<T> callIfRunning(
            Supplier<? extends T> supplier, String supplierDescription) {
        synchronized (lock) {
            if (isRunning()) {
                return Optional.of(supplier.get());
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        supplierDescription);
                return Optional.empty();
            }
        }
    }

    @GuardedBy("lock")
    private boolean isRunning() {
        return state == State.RUNNING;
    }

    private CompletableFuture<Void> runIfValidLeader(
            UUID expectedLeaderId, Runnable action, Runnable noLeaderFallback) {
        synchronized (lock) {
            if (isRunning() && leaderElection != null) {
                return leaderElection
                        .hasLeadershipAsync(expectedLeaderId)
                        .thenAccept(
                                hasLeadership -> {
                                    synchronized (lock) {
                                        if (isRunning() && hasLeadership) {
                                            action.run();
                                        } else {
                                            noLeaderFallback.run();
                                        }
                                    }
                                });
            } else {
                noLeaderFallback.run();
                return FutureUtils.completedVoidFuture();
            }
        }
    }

    private CompletableFuture<Void> runIfValidLeader(
            UUID expectedLeaderId, Runnable action, String noLeaderFallbackCommandDescription) {
        return runIfValidLeader(
                expectedLeaderId,
                action,
                () ->
                        printLogIfNotValidLeader(
                                noLeaderFallbackCommandDescription, expectedLeaderId));
    }

    private <T> void forwardIfValidLeader(
            UUID expectedLeaderId,
            CompletableFuture<? extends T> source,
            CompletableFuture<T> target,
            String forwardDescription) {
        source.whenComplete(
                (t, throwable) ->
                        runIfValidLeader(
                                expectedLeaderId,
                                () -> {
                                    if (throwable != null) {
                                        target.completeExceptionally(throwable);
                                    } else {
                                        target.complete(t);
                                    }
                                },
                                forwardDescription));
    }

    enum State {
        RUNNING,
        STOPPED,
        JOB_COMPLETED,
    }
}
