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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/** This handler can be used to submit jobs to a Flink cluster. */
public final class JobSubmitHandler
        extends AbstractRestHandler<
                DispatcherGateway,
                JobSubmitRequestBody,
                JobSubmitResponseBody,
                EmptyMessageParameters> {

    private static final String FILE_TYPE_EXECUTION_PLAN = "ExecutionPlan";
    private static final String FILE_TYPE_JAR = "Jar";
    private static final String FILE_TYPE_ARTIFACT = "Artifact";

    private final Executor executor;
    private final Configuration configuration;

    public JobSubmitHandler(
            GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> headers,
            Executor executor,
            Configuration configuration) {
        super(leaderRetriever, timeout, headers, JobSubmitHeaders.getInstance());
        this.executor = executor;
        this.configuration = configuration;
    }

    /**
     * 处理作业提交请求的核心方法。该方法会对上传的文件进行验证，加载执行计划，
     * 上传作业相关文件，最后将作业提交到调度器网关。
     *
     * @param request 包含作业提交请求体和上传文件的处理请求对象
     * @param gateway 调度器网关，用于与调度器进行交互
     * @return 一个 CompletableFuture，异步返回作业提交的响应体
     * @throws RestHandlerException 当请求处理过程中出现错误时抛出此异常
     */
    @Override
    protected CompletableFuture<JobSubmitResponseBody> handleRequest(
            @Nonnull HandlerRequest<JobSubmitRequestBody> request,
            @Nonnull DispatcherGateway gateway)
            throws RestHandlerException {
        // 获取请求中上传的文件集合
        final Collection<File> uploadedFiles = request.getUploadedFiles();
        // 将上传的文件集合转换为映射，键为文件名，值为对应的本地文件路径
        final Map<String, Path> nameToFile =
                uploadedFiles.stream()
                        .collect(Collectors.toMap(File::getName, Path::fromLocalFile));

        // 检查上传文件的数量和映射的大小是否一致，若不一致则抛出异常
        if (uploadedFiles.size() != nameToFile.size()) {
            throw new RestHandlerException(
                    String.format(
                            "The number of uploaded files was %s than the expected count. Expected: %s Actual %s",
                            uploadedFiles.size() < nameToFile.size() ? "lower" : "higher",
                            nameToFile.size(),
                            uploadedFiles.size()),
                    HttpResponseStatus.BAD_REQUEST);
        }

        // 获取作业提交请求体
        final JobSubmitRequestBody requestBody = request.getRequestBody();

        // 检查请求体中的执行计划文件名是否为空，若为空则抛出异常
        if (requestBody.executionPlanFileName == null) {
            throw new RestHandlerException(
                    String.format(
                            "The %s field must not be omitted or be null.",
                            JobSubmitRequestBody.FIELD_NAME_JOB_GRAPH),
                    HttpResponseStatus.BAD_REQUEST);
        }

        // 异步加载执行计划
        CompletableFuture<ExecutionPlan> executionPlanFuture =
                loadExecutionPlan(requestBody, nameToFile);

        // 获取需要上传的 JAR 文件路径集合
        Collection<Path> jarFiles = getJarFilesToUpload(requestBody.jarFileNames, nameToFile);

        // 获取需要上传的分布式缓存文件路径集合
        Collection<Tuple2<String, Path>> artifacts =
                getArtifactFilesToUpload(requestBody.artifactFileNames, nameToFile);

        // 异步上传作业执行计划文件，并返回最终的执行计划
        CompletableFuture<ExecutionPlan> finalizedExecutionPlanFuture =
                uploadExecutionPlanFiles(
                        gateway, executionPlanFuture, jarFiles, artifacts, configuration);

        // 异步提交作业到调度器网关
        CompletableFuture<Acknowledge> jobSubmissionFuture =
                finalizedExecutionPlanFuture.thenCompose(
                        executionPlan -> gateway.submitJob(executionPlan, timeout));

        // 当作业提交成功且执行计划加载完成后，构建并返回作业提交响应体
        return jobSubmissionFuture.thenCombine(
                executionPlanFuture,
                (ack, executionPlan) ->
                        new JobSubmitResponseBody("/jobs/" + executionPlan.getJobID()));
    }

    private CompletableFuture<ExecutionPlan> loadExecutionPlan(
            JobSubmitRequestBody requestBody, Map<String, Path> nameToFile)
            throws MissingFileException {
        final Path executionPlanFile =
                getPathAndAssertUpload(
                        requestBody.executionPlanFileName, FILE_TYPE_EXECUTION_PLAN, nameToFile);

        return CompletableFuture.supplyAsync(
                () -> {
                    ExecutionPlan executionPlan;
                    try (ObjectInputStream objectIn =
                            new ObjectInputStream(
                                    executionPlanFile.getFileSystem().open(executionPlanFile))) {
                        executionPlan = (ExecutionPlan) objectIn.readObject();
                    } catch (Exception e) {
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Failed to deserialize ExecutionPlan.",
                                        HttpResponseStatus.BAD_REQUEST,
                                        e));
                    }
                    return executionPlan;
                },
                executor);
    }

    private static Collection<Path> getJarFilesToUpload(
            Collection<String> jarFileNames, Map<String, Path> nameToFileMap)
            throws MissingFileException {
        Collection<Path> jarFiles = new ArrayList<>(jarFileNames.size());
        for (String jarFileName : jarFileNames) {
            Path jarFile = getPathAndAssertUpload(jarFileName, FILE_TYPE_JAR, nameToFileMap);
            jarFiles.add(new Path(jarFile.toString()));
        }
        return jarFiles;
    }

    private static Collection<Tuple2<String, Path>> getArtifactFilesToUpload(
            Collection<JobSubmitRequestBody.DistributedCacheFile> artifactEntries,
            Map<String, Path> nameToFileMap)
            throws MissingFileException {
        Collection<Tuple2<String, Path>> artifacts = new ArrayList<>(artifactEntries.size());
        for (JobSubmitRequestBody.DistributedCacheFile artifactFileName : artifactEntries) {
            Path artifactFile =
                    getPathAndAssertUpload(
                            artifactFileName.fileName, FILE_TYPE_ARTIFACT, nameToFileMap);
            artifacts.add(Tuple2.of(artifactFileName.entryName, new Path(artifactFile.toString())));
        }
        return artifacts;
    }

    private CompletableFuture<ExecutionPlan> uploadExecutionPlanFiles(
            DispatcherGateway gateway,
            CompletableFuture<ExecutionPlan> executionPlanFuture,
            Collection<Path> jarFiles,
            Collection<Tuple2<String, Path>> artifacts,
            Configuration configuration) {
        CompletableFuture<Integer> blobServerPortFuture = gateway.getBlobServerPort(timeout);

        return executionPlanFuture.thenCombine(
                blobServerPortFuture,
                (ExecutionPlan executionPlan, Integer blobServerPort) -> {
                    final InetSocketAddress address =
                            new InetSocketAddress(gateway.getHostname(), blobServerPort);
                    try {
                        ClientUtils.uploadExecutionPlanFiles(
                                executionPlan,
                                jarFiles,
                                artifacts,
                                () -> new BlobClient(address, configuration));
                    } catch (FlinkException e) {
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Could not upload job files.",
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                        e));
                    }
                    return executionPlan;
                });
    }

    private static Path getPathAndAssertUpload(
            String fileName, String type, Map<String, Path> uploadedFiles)
            throws MissingFileException {
        final Path file = uploadedFiles.get(fileName);
        if (file == null) {
            throw new MissingFileException(type, fileName);
        }
        return file;
    }

    private static final class MissingFileException extends RestHandlerException {

        private static final long serialVersionUID = -7954810495610194965L;

        MissingFileException(String type, String fileName) {
            super(
                    type + " file " + fileName + " could not be found on the server.",
                    HttpResponseStatus.BAD_REQUEST);
        }
    }
}
