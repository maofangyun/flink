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

package org.apache.flink.runtime.rest;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.io.network.netty.InboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.runtime.net.RedirectingSslHandler;
import org.apache.flink.runtime.rest.handler.PipelineErrorHandler;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.router.MultipartRoutes;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.rest.handler.router.RouterHandler;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrapConfig;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** An abstract class for netty-based REST server endpoints. */
public abstract class RestServerEndpoint implements RestService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final Object lock = new Object();

    private final Configuration configuration;
    private final String restAddress;
    private final String restBindAddress;
    private final String restBindPortRange;
    @Nullable private final SSLHandlerFactory sslHandlerFactory;
    private final int maxContentLength;

    protected final Path uploadDir;
    protected final Map<String, String> responseHeaders;

    private final CompletableFuture<Void> terminationFuture;

    private List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers;
    private ServerBootstrap bootstrap;
    private Channel serverChannel;
    private String restBaseUrl;
    private int port;

    private State state = State.CREATED;

    private final List<InboundChannelHandlerFactory> inboundChannelHandlerFactories;

    public RestServerEndpoint(Configuration configuration)
            throws IOException, ConfigurationException {
        Preconditions.checkNotNull(configuration);
        RestServerEndpointConfiguration restConfiguration =
                RestServerEndpointConfiguration.fromConfiguration(configuration);
        Preconditions.checkNotNull(restConfiguration);

        this.configuration = configuration;
        this.restAddress = restConfiguration.getRestAddress();
        this.restBindAddress = restConfiguration.getRestBindAddress();
        this.restBindPortRange = restConfiguration.getRestBindPortRange();
        this.sslHandlerFactory = restConfiguration.getSslHandlerFactory();

        this.uploadDir = restConfiguration.getUploadDir();
        createUploadDir(uploadDir, log, true);

        this.maxContentLength = restConfiguration.getMaxContentLength();
        this.responseHeaders = restConfiguration.getResponseHeaders();

        terminationFuture = new CompletableFuture<>();

        inboundChannelHandlerFactories = new ArrayList<>();
        ServiceLoader<InboundChannelHandlerFactory> loader =
                ServiceLoader.load(InboundChannelHandlerFactory.class);
        final Iterator<InboundChannelHandlerFactory> factories = loader.iterator();
        while (factories.hasNext()) {
            try {
                final InboundChannelHandlerFactory factory = factories.next();
                if (factory != null) {
                    inboundChannelHandlerFactories.add(factory);
                    log.info("Loaded channel inbound factory: {}", factory);
                }
            } catch (Throwable e) {
                log.error("Could not load channel inbound factory.", e);
                throw e;
            }
        }
        inboundChannelHandlerFactories.sort(
                Comparator.comparingInt(InboundChannelHandlerFactory::priority).reversed());
    }

    @VisibleForTesting
    List<InboundChannelHandlerFactory> getInboundChannelHandlerFactories() {
        return inboundChannelHandlerFactories;
    }

    /**
     * This method is called at the beginning of {@link #start()} to setup all handlers that the
     * REST server endpoint implementation requires.
     *
     * @param localAddressFuture future rest address of the RestServerEndpoint
     * @return Collection of AbstractRestHandler which are added to the server endpoint
     */
    protected abstract List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>>
            initializeHandlers(final CompletableFuture<String> localAddressFuture);

    /**
     * 启动此 REST 服务器端点。
     *
     * @throws Exception 如果无法启动 RestServerEndpoint 则抛出异常
     */
    public final void start() throws Exception {
        // 使用锁确保线程安全，避免多个线程同时启动服务器端点
        synchronized (lock) {
            // 检查服务器端点状态，确保其处于初始创建状态，不允许重复启动
            Preconditions.checkState(
                    state == State.CREATED, "The RestServerEndpoint cannot be restarted.");

            // 记录日志，表明开始启动 REST 服务器端点
            log.info("Starting rest endpoint.");

            // 创建一个路由器实例，用于处理不同路径的请求
            // 注意：这里使用了原始类型，建议指定泛型参数以避免潜在问题
            final Router router = new Router();
            // 创建一个 CompletableFuture 用于异步获取 REST 服务器的地址
            final CompletableFuture<String> restAddressFuture = new CompletableFuture<>();

            // 调用抽象方法初始化所有的 REST 处理程序
            handlers = initializeHandlers(restAddressFuture);

            /* 对处理程序进行排序，使其按照以下顺序排列:
             * /jobs
             * /jobs/overview
             * /jobs/:jobid
             * /jobs/:jobid/config
             * /:*
             */
            // 注意：Collections.sort 可以替换为 List.sort 以提高代码的现代性
            Collections.sort(handlers, RestHandlerUrlComparator.INSTANCE);

            // 检查所有端点和处理程序是否唯一，避免重复注册
            checkAllEndpointsAndHandlersAreUnique(handlers);
            // 遍历所有处理程序，将其注册到路由器中
            handlers.forEach(handler -> registerHandler(router, handler, log));

            // 创建多部分路由，用于处理文件上传等多部分请求
            MultipartRoutes multipartRoutes = createMultipartRoutes(handlers);
            // 记录调试日志，显示使用的多部分路由信息
            log.debug("Using {} for FileUploadHandler", multipartRoutes);

            // 创建一个 ChannelInitializer 用于初始化新连接的 Channel 管道
            // 注意：显式类型实参 SocketChannel 可被替换为 <>
            ChannelInitializer<SocketChannel> initializer =
                    new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) throws ConfigurationException {
                            // 创建一个路由器处理程序，用于处理路由请求
                            RouterHandler handler = new RouterHandler(router, responseHeaders);

                            // SSL 处理器应该是管道中的第一个处理器
                            if (isHttpsEnabled()) {
                                ch.pipeline()
                                        .addLast(
                                                "ssl",
                                                new RedirectingSslHandler(
                                                        restAddress,
                                                        restAddressFuture,
                                                        sslHandlerFactory));
                            }

                            // 依次添加 HTTP 服务器编解码器、文件上传处理程序和 HTTP 对象聚合器到管道中
                            ch.pipeline()
                                    .addLast(new HttpServerCodec())
                                    .addLast(new FileUploadHandler(uploadDir, multipartRoutes))
                                    .addLast(
                                            new FlinkHttpObjectAggregator(
                                                    maxContentLength, responseHeaders));

                            // 遍历所有入站通道处理程序工厂，创建并添加处理程序到管道中
                            for (InboundChannelHandlerFactory factory :
                                    inboundChannelHandlerFactories) {
                                // 通过工厂创建通道处理程序
                                Optional<ChannelHandler> channelHandler =
                                        factory.createHandler(configuration, responseHeaders);
                                // 注意：if (channelHandler.isPresent()) 可被替换为函数样式的单个表达式
                                if (channelHandler.isPresent()) {
                                    ch.pipeline().addLast(channelHandler.get());
                                }
                            }

                            // 依次添加分块写入处理程序、路由器处理程序和管道错误处理程序到管道中
                            ch.pipeline()
                                    .addLast(new ChunkedWriteHandler())
                                    .addLast(handler.getName(), handler)
                                    .addLast(new PipelineErrorHandler(log, responseHeaders));
                        }
                    };

            // 创建一个单线程的 NioEventLoopGroup 作为 boss 组，用于接受新连接
            NioEventLoopGroup bossGroup =
                    new NioEventLoopGroup(
                            1, new ExecutorThreadFactory("flink-rest-server-netty-boss"));
            // 创建一个 NioEventLoopGroup 作为 worker 组，用于处理连接的读写操作
            NioEventLoopGroup workerGroup =
                    new NioEventLoopGroup(
                            0, new ExecutorThreadFactory("flink-rest-server-netty-worker"));

            // 创建一个 ServerBootstrap 用于启动 Netty 服务器
            bootstrap = new ServerBootstrap();
            // 配置 ServerBootstrap 的 boss 组和 worker 组，指定通道类型和子处理器
            bootstrap
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(initializer);

            // 从配置的端口范围字符串中获取可用端口的迭代器
            Iterator<Integer> portsIterator;
            try {
                portsIterator = NetUtils.getPortRangeFromString(restBindPortRange);
            } catch (IllegalConfigurationException e) {
                // 重新抛出非法配置异常
                throw e;
            } catch (Exception e) {
                // 若解析端口范围失败，抛出非法参数异常
                throw new IllegalArgumentException(
                        "Invalid port range definition: " + restBindPortRange);
            }

            // 记录最终选择的端口号
            int chosenPort = 0;
            // 遍历可用端口，尝试绑定服务器
            while (portsIterator.hasNext()) {
                try {
                    // 获取下一个可用端口
                    chosenPort = portsIterator.next();
                    final ChannelFuture channel;
                    if (restBindAddress == null) {
                        // 若未指定绑定地址，则绑定到所有可用网络接口
                        channel = bootstrap.bind(chosenPort);
                    } else {
                        // 若指定了绑定地址，则绑定到指定地址和端口
                        channel = bootstrap.bind(restBindAddress, chosenPort);
                    }
                    // 同步等待绑定操作完成，并获取服务器 Channel
                    serverChannel = channel.syncUninterruptibly().channel();
                    // 若绑定成功，跳出循环
                    break;
                } catch (final Exception e) {
                    // syncUninterruptibly() 通过 Unsafe 抛出受检查异常
                    // 若异常不是端口被占用导致的 BindException，则立即失败
                    // 注意：条件 '!(e instanceof java.net.BindException)' 始终为 'true'，可能存在逻辑错误
                    if (!(e instanceof java.net.BindException)) {
                        throw e;
                    }
                }
            }

            // 若遍历所有端口后仍未成功绑定，则抛出 BindException
            if (serverChannel == null) {
                throw new BindException(
                        "Could not start rest endpoint on any port in port range "
                                + restBindPortRange);
            }

            // 记录调试日志，显示绑定的地址和端口
            log.debug("Binding rest endpoint to {}:{}.", restBindAddress, chosenPort);

            // 获取服务器绑定的本地地址
            final InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
            // 确定要公开的服务器地址
            final String advertisedAddress;
            if (bindAddress.getAddress().isAnyLocalAddress()) {
                // 若绑定到任意本地地址，则使用配置的 REST 地址
                advertisedAddress = this.restAddress;
            } else {
                // 否则使用实际绑定的 IP 地址
                advertisedAddress = bindAddress.getAddress().getHostAddress();
            }

            // 记录服务器端口号
            port = bindAddress.getPort();

            // 记录日志，显示 REST 服务器监听的地址和端口
            log.info("Rest endpoint listening at {}:{}", advertisedAddress, port);

            // 构建 REST 服务器的基础 URL
            restBaseUrl = new URL(determineProtocol(), advertisedAddress, port, "").toString();

            // 完成 CompletableFuture，将 REST 基础 URL 传递给相关处理程序
            restAddressFuture.complete(restBaseUrl);

            // 将服务器端点状态设置为运行中
            state = State.RUNNING;

            // 调用抽象方法启动子类特定的服务
            startInternal();
        }
    }

    /**
     * Hook to start sub class specific services.
     *
     * @throws Exception if an error occurred
     */
    protected abstract void startInternal() throws Exception;

    /**
     * Returns the address on which this endpoint is accepting requests.
     *
     * @return address on which this endpoint is accepting requests or null if none
     */
    @Nullable
    public InetSocketAddress getServerAddress() {
        synchronized (lock) {
            assertRestServerHasBeenStarted();
            Channel server = this.serverChannel;

            if (server != null) {
                try {
                    return ((InetSocketAddress) server.localAddress());
                } catch (Exception e) {
                    log.error("Cannot access local server address", e);
                }
            }

            return null;
        }
    }

    /**
     * Returns the base URL of the REST server endpoint.
     *
     * @return REST base URL of this endpoint
     */
    public String getRestBaseUrl() {
        synchronized (lock) {
            assertRestServerHasBeenStarted();
            return restBaseUrl;
        }
    }

    private void assertRestServerHasBeenStarted() {
        Preconditions.checkState(
                state != State.CREATED, "The RestServerEndpoint has not been started yet.");
    }

    @Override
    public int getRestPort() {
        synchronized (lock) {
            assertRestServerHasBeenStarted();
            return port;
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            log.info("Shutting down rest endpoint.");

            if (state == State.RUNNING) {
                final CompletableFuture<Void> shutDownFuture =
                        FutureUtils.composeAfterwards(closeHandlersAsync(), this::shutDownInternal);

                shutDownFuture.whenComplete(
                        (Void ignored, Throwable throwable) -> {
                            log.info("Shut down complete.");
                            if (throwable != null) {
                                terminationFuture.completeExceptionally(throwable);
                            } else {
                                terminationFuture.complete(null);
                            }
                        });
                state = State.SHUTDOWN;
            } else if (state == State.CREATED) {
                terminationFuture.complete(null);
                state = State.SHUTDOWN;
            }

            return terminationFuture;
        }
    }

    private FutureUtils.ConjunctFuture<Void> closeHandlersAsync() {
        return FutureUtils.waitForAll(
                handlers.stream()
                        .map(tuple -> tuple.f1)
                        .filter(handler -> handler instanceof AutoCloseableAsync)
                        .map(handler -> ((AutoCloseableAsync) handler).closeAsync())
                        .collect(Collectors.toList()));
    }

    /**
     * Stops this REST server endpoint.
     *
     * @return Future which is completed once the shut down has been finished.
     */
    protected CompletableFuture<Void> shutDownInternal() {

        synchronized (lock) {
            CompletableFuture<?> channelFuture = new CompletableFuture<>();
            if (serverChannel != null) {
                serverChannel
                        .close()
                        .addListener(
                                finished -> {
                                    if (finished.isSuccess()) {
                                        channelFuture.complete(null);
                                    } else {
                                        channelFuture.completeExceptionally(finished.cause());
                                    }
                                });
                serverChannel = null;
            }

            final CompletableFuture<Void> channelTerminationFuture = new CompletableFuture<>();

            channelFuture.thenRun(
                    () -> {
                        CompletableFuture<?> groupFuture = new CompletableFuture<>();
                        CompletableFuture<?> childGroupFuture = new CompletableFuture<>();
                        final Duration gracePeriod = Duration.ofSeconds(10L);

                        if (bootstrap != null) {
                            final ServerBootstrapConfig config = bootstrap.config();
                            final EventLoopGroup group = config.group();
                            if (group != null) {
                                group.shutdownGracefully(
                                                0L, gracePeriod.toMillis(), TimeUnit.MILLISECONDS)
                                        .addListener(
                                                finished -> {
                                                    if (finished.isSuccess()) {
                                                        groupFuture.complete(null);
                                                    } else {
                                                        groupFuture.completeExceptionally(
                                                                finished.cause());
                                                    }
                                                });
                            } else {
                                groupFuture.complete(null);
                            }

                            final EventLoopGroup childGroup = config.childGroup();
                            if (childGroup != null) {
                                childGroup
                                        .shutdownGracefully(
                                                0L, gracePeriod.toMillis(), TimeUnit.MILLISECONDS)
                                        .addListener(
                                                finished -> {
                                                    if (finished.isSuccess()) {
                                                        childGroupFuture.complete(null);
                                                    } else {
                                                        childGroupFuture.completeExceptionally(
                                                                finished.cause());
                                                    }
                                                });
                            } else {
                                childGroupFuture.complete(null);
                            }

                            bootstrap = null;
                        } else {
                            // complete the group futures since there is nothing to stop
                            groupFuture.complete(null);
                            childGroupFuture.complete(null);
                        }

                        CompletableFuture<Void> combinedFuture =
                                FutureUtils.completeAll(
                                        Arrays.asList(groupFuture, childGroupFuture));

                        combinedFuture.whenComplete(
                                (Void ignored, Throwable throwable) -> {
                                    if (throwable != null) {
                                        channelTerminationFuture.completeExceptionally(throwable);
                                    } else {
                                        channelTerminationFuture.complete(null);
                                    }
                                });
                    });

            return channelTerminationFuture;
        }
    }

    private boolean isHttpsEnabled() {
        return sslHandlerFactory != null;
    }

    private String determineProtocol() {
        return isHttpsEnabled() ? "https" : "http";
    }

    private static void registerHandler(
            Router router,
            Tuple2<RestHandlerSpecification, ChannelInboundHandler> specificationHandler,
            Logger log) {
        for (String route : getHandlerRoutes(specificationHandler.f0)) {
            log.debug(
                    "Register handler {} under {}@{}.",
                    specificationHandler.f1,
                    specificationHandler.f0.getHttpMethod(),
                    route);
            registerHandler(
                    router,
                    route,
                    specificationHandler.f0.getHttpMethod(),
                    specificationHandler.f1);
        }
    }

    private static void registerHandler(
            Router router,
            String handlerURL,
            HttpMethodWrapper httpMethod,
            ChannelInboundHandler handler) {
        switch (httpMethod) {
            case GET:
                router.addGet(handlerURL, handler);
                break;
            case POST:
                router.addPost(handlerURL, handler);
                break;
            case DELETE:
                router.addDelete(handlerURL, handler);
                break;
            case PATCH:
                router.addPatch(handlerURL, handler);
                break;
            case PUT:
                router.addPut(handlerURL, handler);
                break;
            default:
                throw new RuntimeException("Unsupported http method: " + httpMethod + '.');
        }
    }

    /** Creates the upload dir if needed. */
    static void createUploadDir(
            final Path uploadDir, final Logger log, final boolean initialCreation)
            throws IOException {
        if (!Files.exists(uploadDir)) {
            if (initialCreation) {
                log.info("Upload directory {} does not exist. ", uploadDir);
            } else {
                log.warn(
                        "Upload directory {} has been deleted externally. "
                                + "Previously uploaded files are no longer available.",
                        uploadDir);
            }
            checkAndCreateUploadDir(uploadDir, log);
        }
    }

    /**
     * Checks whether the given directory exists and is writable. If it doesn't exist, this method
     * will attempt to create it.
     *
     * @param uploadDir directory to check
     * @param log logger used for logging output
     * @throws IOException if the directory does not exist and cannot be created, or if the
     *     directory isn't writable
     */
    private static synchronized void checkAndCreateUploadDir(final Path uploadDir, final Logger log)
            throws IOException {
        if (Files.exists(uploadDir) && Files.isWritable(uploadDir)) {
            log.info("Using directory {} for file uploads.", uploadDir);
        } else if (Files.isWritable(Files.createDirectories(uploadDir))) {
            log.info("Created directory {} for file uploads.", uploadDir);
        } else {
            log.warn("Upload directory {} cannot be created or is not writable.", uploadDir);
            throw new IOException(
                    String.format(
                            "Upload directory %s cannot be created or is not writable.",
                            uploadDir));
        }
    }

    private static void checkAllEndpointsAndHandlersAreUnique(
            final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers) {
        // check for all handlers that
        // 1) the instance is only registered once
        // 2) only 1 handler is registered for each endpoint (defined by (version, method, url))
        // technically the first check is redundant since a duplicate instance also returns the same
        // headers which
        // should fail the second check, but we get a better error message
        final Set<String> uniqueEndpoints = new HashSet<>();
        final Set<ChannelInboundHandler> distinctHandlers =
                Collections.newSetFromMap(new IdentityHashMap<>());
        for (Tuple2<RestHandlerSpecification, ChannelInboundHandler> handler : handlers) {
            boolean isNewHandler = distinctHandlers.add(handler.f1);
            if (!isNewHandler) {
                throw new FlinkRuntimeException(
                        "Duplicate REST handler instance found."
                                + " Please ensure each instance is registered only once.");
            }

            final RestHandlerSpecification headers = handler.f0;
            for (RestAPIVersion supportedRestAPIVersion : headers.getSupportedAPIVersions()) {
                final String parameterizedEndpoint =
                        supportedRestAPIVersion.toString()
                                + headers.getHttpMethod()
                                + headers.getTargetRestEndpointURL();
                // normalize path parameters; distinct path parameters still clash at runtime
                final String normalizedEndpoint =
                        parameterizedEndpoint.replaceAll(":[\\w-]+", ":param");
                boolean isNewEndpoint = uniqueEndpoints.add(normalizedEndpoint);
                if (!isNewEndpoint) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "REST handler registration overlaps with another registration for: version=%s, method=%s, url=%s.",
                                    supportedRestAPIVersion,
                                    headers.getHttpMethod(),
                                    headers.getTargetRestEndpointURL()));
                }
            }
        }
    }

    private MultipartRoutes createMultipartRoutes(
            List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers) {
        MultipartRoutes.Builder builder = new MultipartRoutes.Builder();

        for (Tuple2<RestHandlerSpecification, ChannelInboundHandler> handler : handlers) {
            if (handler.f0.getHttpMethod() == HttpMethodWrapper.POST) {
                for (String url : getHandlerRoutes(handler.f0)) {
                    builder.addPostRoute(url);
                }
            }

            // The cast is necessary, because currently only UntypedResponseMessageHeaders exposes
            // whether the handler accepts file uploads.
            if (handler.f0 instanceof UntypedResponseMessageHeaders) {
                UntypedResponseMessageHeaders<?, ?> header =
                        (UntypedResponseMessageHeaders<?, ?>) handler.f0;
                if (header.acceptsFileUploads()) {
                    for (String url : getHandlerRoutes(header)) {
                        builder.addFileUploadRoute(url);
                    }
                }
            }
        }
        return builder.build();
    }

    private static Iterable<String> getHandlerRoutes(RestHandlerSpecification handlerSpec) {
        final List<String> registeredRoutes = new ArrayList<>();
        final String handlerUrl = handlerSpec.getTargetRestEndpointURL();
        for (RestAPIVersion<?> supportedVersion : handlerSpec.getSupportedAPIVersions()) {
            String versionedUrl = '/' + supportedVersion.getURLVersionPrefix() + handlerUrl;
            registeredRoutes.add(versionedUrl);

            if (supportedVersion.isDefaultVersion()) {
                // Unversioned URL for convenience and backwards compatibility
                registeredRoutes.add(handlerUrl);
            }
        }
        return registeredRoutes;
    }

    /**
     * Comparator for Rest URLs.
     *
     * <p>The comparator orders the Rest URLs such that URLs with path parameters are ordered behind
     * those without parameters. E.g.: /jobs /jobs/overview /jobs/:jobid /jobs/:jobid/config /:*
     *
     * <p>IMPORTANT: This comparator is highly specific to how Netty path parameters are encoded.
     * Namely with a preceding ':' character.
     */
    public static final class RestHandlerUrlComparator
            implements Comparator<Tuple2<RestHandlerSpecification, ChannelInboundHandler>>,
                    Serializable {

        private static final long serialVersionUID = 2388466767835547926L;

        private static final Comparator<String> CASE_INSENSITIVE_ORDER =
                new CaseInsensitiveOrderComparator();

        static final RestHandlerUrlComparator INSTANCE = new RestHandlerUrlComparator();

        @Override
        public int compare(
                Tuple2<RestHandlerSpecification, ChannelInboundHandler> o1,
                Tuple2<RestHandlerSpecification, ChannelInboundHandler> o2) {
            final int urlComparisonResult =
                    CASE_INSENSITIVE_ORDER.compare(
                            o1.f0.getTargetRestEndpointURL(), o2.f0.getTargetRestEndpointURL());
            if (urlComparisonResult != 0) {
                return urlComparisonResult;
            } else {
                Collection<? extends RestAPIVersion> o1APIVersions =
                        o1.f0.getSupportedAPIVersions();
                RestAPIVersion o1Version = Collections.min(o1APIVersions);
                Collection<? extends RestAPIVersion> o2APIVersions =
                        o2.f0.getSupportedAPIVersions();
                RestAPIVersion o2Version = Collections.min(o2APIVersions);
                return o1Version.compareTo(o2Version);
            }
        }

        /**
         * Comparator for Rest URLs.
         *
         * <p>The comparator orders the Rest URLs such that URLs with path parameters are ordered
         * behind those without parameters. E.g.: /jobs /jobs/overview /jobs/:jobid
         * /jobs/:jobid/config /:*
         *
         * <p>IMPORTANT: This comparator is highly specific to how Netty path parameters are
         * encoded. Namely with a preceding ':' character.
         */
        public static final class CaseInsensitiveOrderComparator
                implements Comparator<String>, Serializable {
            private static final long serialVersionUID = 8550835445193437027L;

            @Override
            public int compare(String s1, String s2) {
                int n1 = s1.length();
                int n2 = s2.length();
                int min = Math.min(n1, n2);
                for (int i = 0; i < min; i++) {
                    char c1 = s1.charAt(i);
                    char c2 = s2.charAt(i);
                    if (c1 != c2) {
                        c1 = Character.toUpperCase(c1);
                        c2 = Character.toUpperCase(c2);
                        if (c1 != c2) {
                            c1 = Character.toLowerCase(c1);
                            c2 = Character.toLowerCase(c2);
                            if (c1 != c2) {
                                if (c1 == ':') {
                                    // c2 is less than c1 because it is also different
                                    return 1;
                                } else if (c2 == ':') {
                                    // c1 is less than c2
                                    return -1;
                                } else {
                                    return c1 - c2;
                                }
                            }
                        }
                    }
                }
                return n1 - n2;
            }
        }
    }

    private enum State {
        CREATED,
        RUNNING,
        SHUTDOWN
    }
}
