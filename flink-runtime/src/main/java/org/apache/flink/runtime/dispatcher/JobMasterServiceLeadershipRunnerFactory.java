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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.DefaultSlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.jobmaster.JobMasterServiceLeadershipRunner;
import org.apache.flink.runtime.jobmaster.SlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Factory which creates a {@link JobMasterServiceLeadershipRunner}. */
public enum JobMasterServiceLeadershipRunnerFactory implements JobManagerRunnerFactory {
    INSTANCE;

    /**
     * 创建一个 JobManagerRunner 实例，用于管理 JobMaster 服务的领导权。
     *
     * @param executionPlan 作业的执行计划，包含作业的详细信息。
     * @param configuration 作业的配置信息。
     * @param rpcService RPC 服务，用于进程间通信。
     * @param highAvailabilityServices 高可用服务，用于处理领导选举和状态存储。
     * @param heartbeatServices 心跳服务，用于监控节点的存活状态。
     * @param jobManagerServices JobManager 的共享服务。
     * @param jobManagerJobMetricGroupFactory 作业指标组工厂，用于创建作业相关的指标组。
     * @param fatalErrorHandler 致命错误处理器，用于处理不可恢复的错误。
     * @param failureEnrichers 失败信息增强器集合，用于丰富失败信息。
     * @param initializationTimestamp 初始化时间戳。
     * @return 一个新的 JobManagerRunner 实例。
     * @throws Exception 如果在创建过程中发生错误。
     */
    @Override
    public JobManagerRunner createJobManagerRunner(
            ExecutionPlan executionPlan,
            Configuration configuration,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            JobManagerSharedServices jobManagerServices,
            JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
            FatalErrorHandler fatalErrorHandler,
            Collection<FailureEnricher> failureEnrichers,
            long initializationTimestamp)
            throws Exception {

        // 检查给定的作业是否为空，如果为空则抛出异常
        checkArgument(!executionPlan.isEmpty(), "The given job is empty");

        // 从配置信息中创建 JobMaster 配置
        final JobMasterConfiguration jobMasterConfiguration =
                JobMasterConfiguration.fromConfiguration(configuration);

        // 从高可用服务中获取作业结果存储
        final JobResultStore jobResultStore = highAvailabilityServices.getJobResultStore();

        // 从高可用服务中获取 JobManager 的领导选举实例
        final LeaderElection jobManagerLeaderElection =
                highAvailabilityServices.getJobManagerLeaderElection(executionPlan.getJobID());

        // 根据配置信息、作业类型和是否动态作业创建 Slot 池服务调度器工厂
        final SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        configuration, executionPlan.getJobType(), executionPlan.isDynamic());

        // 如果调度模式为 REACTIVE，则检查调度器类型是否为 Adaptive
        if (jobMasterConfiguration.getConfiguration().get(JobManagerOptions.SCHEDULER_MODE)
                == SchedulerExecutionMode.REACTIVE) {
            Preconditions.checkState(
                    slotPoolServiceSchedulerFactory.getSchedulerType()
                            == JobManagerOptions.SchedulerType.Adaptive,
                    "Adaptive Scheduler is required for reactive mode");
        }

        // 从库缓存管理器中注册类加载器租约
        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                jobManagerServices
                        .getLibraryCacheManager()
                        .registerClassLoaderLease(executionPlan.getJobID());

        // 获取或解析用户代码类加载器
        final ClassLoader userCodeClassLoader =
                classLoaderLease
                        .getOrResolveClassLoader(
                                executionPlan.getUserJarBlobKeys(), executionPlan.getClasspaths())
                        .asClassLoader();

        // 如果执行计划是 StreamGraph 类型，则反序列化用户定义的实例
        if (executionPlan instanceof StreamGraph) {
            ((StreamGraph) executionPlan)
                    .deserializeUserDefinedInstances(
                            userCodeClassLoader, jobManagerServices.getFutureExecutor());
        }

        // 创建默认的 JobMaster 服务工厂
        final DefaultJobMasterServiceFactory jobMasterServiceFactory =
                new DefaultJobMasterServiceFactory(
                        MdcUtils.scopeToJob(
                                executionPlan.getJobID(), jobManagerServices.getIoExecutor()),
                        rpcService,
                        jobMasterConfiguration,
                        executionPlan,
                        highAvailabilityServices,
                        slotPoolServiceSchedulerFactory,
                        jobManagerServices,
                        heartbeatServices,
                        jobManagerJobMetricGroupFactory,
                        fatalErrorHandler,
                        userCodeClassLoader,
                        failureEnrichers,
                        initializationTimestamp);
        // 创建默认的 JobMaster 服务进程工厂
        final DefaultJobMasterServiceProcessFactory jobMasterServiceProcessFactory =
                new DefaultJobMasterServiceProcessFactory(
                        executionPlan.getJobID(),
                        executionPlan.getName(),
                        executionPlan.getJobType(),
                        executionPlan.getCheckpointingSettings(),
                        initializationTimestamp,
                        jobMasterServiceFactory);
        // 创建 JobMaster 服务领导权运行器
        return new JobMasterServiceLeadershipRunner(
                jobMasterServiceProcessFactory,
                jobManagerLeaderElection,
                jobResultStore,
                classLoaderLease,
                fatalErrorHandler);
    }
}
