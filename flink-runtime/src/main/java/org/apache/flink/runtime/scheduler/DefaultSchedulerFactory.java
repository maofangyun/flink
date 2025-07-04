/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.adaptivebatch.NonAdaptiveExecutionPlanSchedulingContext;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.scheduler.DefaultSchedulerComponents.createSchedulerComponents;
import static org.apache.flink.runtime.scheduler.SchedulerBase.computeVertexParallelismStore;

/** Factory for {@link DefaultScheduler}. */
public class DefaultSchedulerFactory implements SchedulerNGFactory {

    @Override
    public SchedulerNG createInstance(
            final Logger log,
            final ExecutionPlan executionPlan,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final SlotPoolService slotPoolService,
            final ScheduledExecutorService futureExecutor,
            final ClassLoader userCodeLoader,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final Duration rpcTimeout,
            final BlobWriter blobWriter,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final Duration slotRequestTimeout,
            final ShuffleMaster<?> shuffleMaster,
            final JobMasterPartitionTracker partitionTracker,
            final ExecutionDeploymentTracker executionDeploymentTracker,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final FatalErrorHandler fatalErrorHandler,
            final JobStatusListener jobStatusListener,
            final Collection<FailureEnricher> failureEnrichers,
            final BlocklistOperations blocklistOperations)
            throws Exception {
        JobGraph jobGraph;
        // 根据执行计划的类型获取作业图
        if (executionPlan instanceof JobGraph) {
            // 若执行计划本身就是 JobGraph 类型，直接赋值
            jobGraph = (JobGraph) executionPlan;
        } else if (executionPlan instanceof StreamGraph) {
            // 若执行计划是 StreamGraph 类型，调用 getJobGraph 方法转换为 JobGraph
            jobGraph = ((StreamGraph) executionPlan).getJobGraph(userCodeLoader);
        } else {
            throw new FlinkException(
                    "Unsupported execution plan " + executionPlan.getClass().getCanonicalName());
        }

        // 从插槽池服务中获取 SlotPool 实例，若无法获取则抛出异常
        final SlotPool slotPool =
                slotPoolService
                        .castInto(SlotPool.class)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "The DefaultScheduler requires a SlotPool."));

        // 创建调度组件，包含启动动作、调度策略工厂和分配器工厂等
        final DefaultSchedulerComponents schedulerComponents =
                createSchedulerComponents(
                        jobGraph.getJobType(),
                        jobGraph.isApproximateLocalRecoveryEnabled(),
                        jobMasterConfiguration,
                        slotPool,
                        slotRequestTimeout);
        // 根据作业配置和检查点启用状态创建重启退避时间策略
        final RestartBackoffTimeStrategy restartBackoffTimeStrategy =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                                jobGraph.getJobConfiguration(),
                                jobMasterConfiguration,
                                jobGraph.isCheckpointingEnabled())
                        .create();
        log.info(
                "Using restart back off time strategy {} for {} ({}).",
                restartBackoffTimeStrategy,
                jobGraph.getName(),
                jobGraph.getJobID());

        // 创建执行图工厂，用于生成执行图
        final ExecutionGraphFactory executionGraphFactory =
                new DefaultExecutionGraphFactory(
                        jobMasterConfiguration,
                        userCodeLoader,
                        executionDeploymentTracker,
                        futureExecutor,
                        ioExecutor,
                        rpcTimeout,
                        jobManagerJobMetricGroup,
                        blobWriter,
                        shuffleMaster,
                        partitionTracker);

        // 创建检查点清理器，根据配置决定清理模式
        final CheckpointsCleaner checkpointsCleaner =
                new CheckpointsCleaner(
                        jobMasterConfiguration.get(CheckpointingOptions.CLEANER_PARALLEL_MODE));

        // 创建并返回 DefaultScheduler 实例
        return new DefaultScheduler(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                schedulerComponents.getStartUpAction(),
                new ScheduledExecutorServiceAdapter(futureExecutor),
                userCodeLoader,
                checkpointsCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                schedulerComponents.getSchedulingStrategyFactory(),
                FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(jobMasterConfiguration),
                restartBackoffTimeStrategy,
                new DefaultExecutionOperations(),
                new ExecutionVertexVersioner(),
                schedulerComponents.getAllocatorFactory(),
                initializationTimestamp,
                mainThreadExecutor,
                (jobId, jobStatus, timestamp) -> {
                    if (jobStatus == JobStatus.RESTARTING) {
                        slotPool.setIsJobRestarting(true);
                    } else {
                        slotPool.setIsJobRestarting(false);
                    }
                    jobStatusListener.jobStatusChanges(jobId, jobStatus, timestamp);
                },
                failureEnrichers,
                executionGraphFactory,
                shuffleMaster,
                rpcTimeout,
                computeVertexParallelismStore(jobGraph),
                new DefaultExecutionDeployer.Factory(),
                NonAdaptiveExecutionPlanSchedulingContext.INSTANCE);
    }

    @Override
    public JobManagerOptions.SchedulerType getSchedulerType() {
        return JobManagerOptions.SchedulerType.Default;
    }
}
