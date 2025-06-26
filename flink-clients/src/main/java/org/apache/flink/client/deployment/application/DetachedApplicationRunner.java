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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.deployment.application.executors.WebSubmissionExecutorServiceLoader;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@link ApplicationRunner} which runs the user specified application using the {@link
 * EmbeddedExecutor}. This runner invokes methods of the provided {@link DispatcherGateway}
 * directly, and it does not go through the REST API.
 *
 * <p>In addition, this runner does not wait for the application to finish, but it submits the
 * application in a {@code DETACHED} mode. As a consequence, applications with jobs that rely on
 * operations like {@code [collect, print, printToErr, count]} will fail.
 */
@Internal
public class DetachedApplicationRunner implements ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(DetachedApplicationRunner.class);

    private final boolean enforceSingleJobExecution;

    public DetachedApplicationRunner(final boolean enforceSingleJobExecution) {
        this.enforceSingleJobExecution = enforceSingleJobExecution;
    }

    @Override
    public List<JobID> run(
            final DispatcherGateway dispatcherGateway,
            final PackagedProgram program,
            final Configuration configuration) {
        checkNotNull(dispatcherGateway);
        checkNotNull(program);
        checkNotNull(configuration);
        return tryExecuteJobs(dispatcherGateway, program, configuration);
    }

    /**
     * 尝试执行应用程序中的作业，并返回作业ID列表。
     * 该方法会将应用程序以分离模式提交，不等待应用程序完成。
     *
     * @param dispatcherGateway 调度器网关，用于与调度器进行交互
     * @param program 打包好的用户程序
     * @param configuration 应用程序的配置信息
     * @return 执行的作业ID列表
     */
    private List<JobID> tryExecuteJobs(
            final DispatcherGateway dispatcherGateway,
            final PackagedProgram program,
            final Configuration configuration) {
        // 将部署模式设置为分离模式，即不等待应用程序完成
        configuration.set(DeploymentOptions.ATTACHED, false);

        // 初始化一个列表，用于存储应用程序中所有作业的ID
        final List<JobID> applicationJobIds = new ArrayList<>();
        // 创建一个管道执行器服务加载器，用于加载执行器服务，并将作业ID列表和调度器网关传递给它
        final PipelineExecutorServiceLoader executorServiceLoader =
                new WebSubmissionExecutorServiceLoader(applicationJobIds, dispatcherGateway);
        // 返回执行的作业ID列表
            // 抛出Flink运行时异常，封装原始异常信息
            // 记录警告日志，表明无法执行应用程序，并打印异常信息
            // 调用ClientUtils的executeProgram方法来执行用户程序
            // 传入执行器服务加载器、配置信息、用户程序、是否强制单作业执行以及是否抑制标准输出
        try {
            ClientUtils.executeProgram(
                    executorServiceLoader, configuration, program, enforceSingleJobExecution, true);
        } catch (ProgramInvocationException e) {
            LOG.warn("Could not execute application: ", e);
            throw new FlinkRuntimeException("Could not execute application.", e);
        }

        return applicationJobIds;
    }
}
