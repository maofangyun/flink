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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Contains utility methods for clients. */
public enum ClientUtils {
    ;

    /**
     * 从给定的 {@link ExecutionPlan} 中提取执行所需的所有文件，并使用给定 {@link Supplier} 提供的 {@link BlobClient} 进行上传。
     *
     * @param executionPlan 执行所需文件的执行计划
     * @param clientSupplier 用于上传文件的 BlobClient 提供者
     * @throws FlinkException 如果上传失败则抛出此异常
     */
    public static void extractAndUploadExecutionPlanFiles(
            ExecutionPlan executionPlan,
            SupplierWithException<BlobClient, IOException> clientSupplier)
            throws FlinkException {
        // 从执行计划中提取用户需要上传的 JAR 文件列表
        List<Path> userJars = executionPlan.getUserJars();
        // 从执行计划中提取用户需要上传的制品信息，将其转换为包含名称和路径的元组集合
        Collection<Tuple2<String, Path>> userArtifacts =
                executionPlan.getUserArtifacts().entrySet().stream()
                        // 遍历执行计划中用户制品的键值对，将其转换为包含名称和路径的元组
                        .map(
                                entry ->
                                        Tuple2.of(
                                                entry.getKey(),
                                                new Path(entry.getValue().filePath)))
                        // 将转换后的元素收集到列表中
                        .collect(Collectors.toList());

        // 调用 uploadExecutionPlanFiles 方法，上传提取出的 JAR 文件和制品
        uploadExecutionPlanFiles(executionPlan, userJars, userArtifacts, clientSupplier);
    }

    /**
     * 使用给定 Supplier 提供的 BlobClient，上传执行给定 ExecutionPlan 所需的 JAR 文件和制品。
     *
     * @param executionPlan 需要上传文件的执行计划
     * @param userJars 需要上传的 JAR 文件集合
     * @param userArtifacts 需要上传的制品集合，每个制品由名称和路径的元组表示
     * @param clientSupplier 用于提供 BlobClient 的供应商，BlobClient 用于上传文件
     * @throws FlinkException 如果上传失败则抛出此异常
     */
    public static void uploadExecutionPlanFiles(
            ExecutionPlan executionPlan,
            Collection<Path> userJars,
            Collection<Tuple2<String, org.apache.flink.core.fs.Path>> userArtifacts,
            SupplierWithException<BlobClient, IOException> clientSupplier)
            throws FlinkException {
        // 检查是否有需要上传的 JAR 文件或制品
        if (!userJars.isEmpty() || !userArtifacts.isEmpty()) {
            // 使用 try-with-resources 语句确保 BlobClient 最终会被关闭
            try (BlobClient client = clientSupplier.get()) {
                // 上传 JAR 文件并将生成的 BlobKey 设置到执行计划中
                uploadAndSetUserJars(executionPlan, userJars, client);
                // 上传制品并将生成的 BlobKey 设置到执行计划中
                uploadAndSetUserArtifacts(executionPlan, userArtifacts, client);
            } catch (IOException ioe) {
                // 若上传过程中发生 IO 异常，将其包装为 FlinkException 抛出
                throw new FlinkException("Could not upload job files.", ioe);
            }
        }
        // 将用户制品条目写入执行计划的配置中
        executionPlan.writeUserArtifactEntriesToConfiguration();
    }

    /**
     * Uploads the given user jars using the given {@link BlobClient}, and sets the appropriate
     * blobkeys on the given {@link ExecutionPlan}.
     *
     * @param executionPlan executionPlan requiring user jars
     * @param userJars jars to upload
     * @param blobClient client to upload jars with
     * @throws IOException if the upload fails
     */
    private static void uploadAndSetUserJars(
            ExecutionPlan executionPlan, Collection<Path> userJars, BlobClient blobClient)
            throws IOException {
        Collection<PermanentBlobKey> blobKeys =
                uploadUserJars(executionPlan.getJobID(), userJars, blobClient);
        setUserJarBlobKeys(blobKeys, executionPlan);
    }

    private static Collection<PermanentBlobKey> uploadUserJars(
            JobID jobId, Collection<Path> userJars, BlobClient blobClient) throws IOException {
        Collection<PermanentBlobKey> blobKeys = new ArrayList<>(userJars.size());
        for (Path jar : userJars) {
            final PermanentBlobKey blobKey = blobClient.uploadFile(jobId, jar);
            blobKeys.add(blobKey);
        }
        return blobKeys;
    }

    private static void setUserJarBlobKeys(
            Collection<PermanentBlobKey> blobKeys, ExecutionPlan executionPlan) {
        blobKeys.forEach(executionPlan::addUserJarBlobKey);
    }

    /**
     * Uploads the given user artifacts using the given {@link BlobClient}, and sets the appropriate
     * blobkeys on the given {@link ExecutionPlan}.
     *
     * @param executionPlan executionPlan requiring user artifacts
     * @param artifactPaths artifacts to upload
     * @param blobClient client to upload artifacts with
     * @throws IOException if the upload fails
     */
    private static void uploadAndSetUserArtifacts(
            ExecutionPlan executionPlan,
            Collection<Tuple2<String, Path>> artifactPaths,
            BlobClient blobClient)
            throws IOException {
        Collection<Tuple2<String, PermanentBlobKey>> blobKeys =
                uploadUserArtifacts(executionPlan.getJobID(), artifactPaths, blobClient);
        setUserArtifactBlobKeys(executionPlan, blobKeys);
    }

    private static Collection<Tuple2<String, PermanentBlobKey>> uploadUserArtifacts(
            JobID jobID, Collection<Tuple2<String, Path>> userArtifacts, BlobClient blobClient)
            throws IOException {
        Collection<Tuple2<String, PermanentBlobKey>> blobKeys =
                new ArrayList<>(userArtifacts.size());
        for (Tuple2<String, Path> userArtifact : userArtifacts) {
            // only upload local files
            if (!userArtifact.f1.getFileSystem().isDistributedFS()) {
                final PermanentBlobKey blobKey = blobClient.uploadFile(jobID, userArtifact.f1);
                blobKeys.add(Tuple2.of(userArtifact.f0, blobKey));
            }
        }
        return blobKeys;
    }

    private static void setUserArtifactBlobKeys(
            ExecutionPlan executionPlan, Collection<Tuple2<String, PermanentBlobKey>> blobKeys)
            throws IOException {
        for (Tuple2<String, PermanentBlobKey> blobKey : blobKeys) {
            executionPlan.setUserArtifactBlobKey(blobKey.f0, blobKey.f1);
        }
    }
}
