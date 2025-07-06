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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.jobmanager.JobPersistenceComponentFactory;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * {@link DispatcherRunnerFactory} implementation which creates {@link DefaultDispatcherRunner}
 * instances.
 */
public class DefaultDispatcherRunnerFactory implements DispatcherRunnerFactory {
    private final DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory;

    public DefaultDispatcherRunnerFactory(
            DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory) {
        this.dispatcherLeaderProcessFactoryFactory = dispatcherLeaderProcessFactoryFactory;
    }

    /**
     * 根据传入的参数创建一个 {@link DispatcherRunner} 实例。
     * 此方法会先通过工厂创建一个 {@link DispatcherLeaderProcessFactory}，
     * 再使用该工厂创建最终的 {@link DispatcherRunner} 实例。
     *
     * @param leaderElection 用于调度器的领导者选举服务，负责选举出调度器的领导者
     * @param fatalErrorHandler 致命错误处理器，用于处理运行过程中出现的致命错误
     * @param jobPersistenceComponentFactory 作业持久化组件工厂，用于创建作业持久化相关组件
     * @param ioExecutor 用于执行 I/O 操作的执行器
     * @param rpcService RPC 服务，用于处理远程过程调用
     * @param partialDispatcherServices 部分调度器服务，包含调度器运行所需的部分服务
     * @return 新创建的 {@link DispatcherRunner} 实例
     * @throws Exception 创建过程中可能抛出的异常
     */
    @Override
    public DispatcherRunner createDispatcherRunner(
            LeaderElection leaderElection,
            FatalErrorHandler fatalErrorHandler,
            JobPersistenceComponentFactory jobPersistenceComponentFactory,
            Executor ioExecutor,
            RpcService rpcService,
            PartialDispatcherServices partialDispatcherServices)
            throws Exception {

        // 通过 DispatcherLeaderProcessFactoryFactory 创建一个 DispatcherLeaderProcessFactory 实例
        // 该实例将用于后续创建 DispatcherRunner
        final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory =
                dispatcherLeaderProcessFactoryFactory.createFactory(
                        jobPersistenceComponentFactory,
                        ioExecutor,
                        rpcService,
                        partialDispatcherServices,
                        fatalErrorHandler);

        // 调用 DefaultDispatcherRunner 的静态创建方法，传入领导者选举服务、致命错误处理器和 DispatcherLeaderProcessFactory
        // 创建并返回一个 DefaultDispatcherRunner 实例
        return DefaultDispatcherRunner.create(
                leaderElection, fatalErrorHandler, dispatcherLeaderProcessFactory);
    }

    public static DefaultDispatcherRunnerFactory createSessionRunner(
            DispatcherFactory dispatcherFactory) {
        return new DefaultDispatcherRunnerFactory(
                SessionDispatcherLeaderProcessFactoryFactory.create(dispatcherFactory));
    }
}
