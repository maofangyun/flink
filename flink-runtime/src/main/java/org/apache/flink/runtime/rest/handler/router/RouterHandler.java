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

package org.apache.flink.runtime.rest.handler.router;

import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpUtil;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Inbound handler that converts HttpRequest to Routed and passes Routed to the matched handler.
 *
 * <p>This class replaces the standard error response to be identical with those sent by the {@link
 * AbstractRestHandler}.
 *
 * <p>This class is based on:
 * https://github.com/sinetja/netty-router/blob/1.10/src/main/java/io/netty/handler/codec/http/router/AbstractHandler.java
 * https://github.com/sinetja/netty-router/blob/1.10/src/main/java/io/netty/handler/codec/http/router/Handler.java
 */
public class RouterHandler extends SimpleChannelInboundHandler<HttpRequest> {
    private static final String ROUTER_HANDLER_NAME =
            RouterHandler.class.getName() + "_ROUTER_HANDLER";
    private static final String ROUTED_HANDLER_NAME =
            RouterHandler.class.getName() + "_ROUTED_HANDLER";

    private static final Logger LOG = LoggerFactory.getLogger(RouterHandler.class);

    private final Map<String, String> responseHeaders;
    private final Router router;

    public RouterHandler(Router router, final Map<String, String> responseHeaders) {
        this.router = requireNonNull(router);
        this.responseHeaders = requireNonNull(responseHeaders);
    }

    public String getName() {
        return ROUTER_HANDLER_NAME;
    }

    /**
     * 处理接收到的 HTTP 请求。该方法会先检查是否为 100 Continue 请求，若是则发送继续响应；
     * 否则对请求进行路由，根据路由结果决定是调用匹配的处理程序还是返回 404 错误响应。
     *
     * @param channelHandlerContext 通道处理程序上下文，用于与通道进行交互
     * @param httpRequest 接收到的 HTTP 请求对象
     */
    @Override
    protected void channelRead0(
            ChannelHandlerContext channelHandlerContext, HttpRequest httpRequest) {
        // 检查 HTTP 请求是否包含 100 Continue 期望头
        if (HttpUtil.is100ContinueExpected(httpRequest)) {
            // 若包含，则向客户端发送 HTTP 100 Continue 响应
            channelHandlerContext.writeAndFlush(
                    new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
            // 处理完毕，直接返回
            return;
        }

        // 开始路由请求
        // 获取 HTTP 请求的方法（如 GET、POST 等）
        HttpMethod method = httpRequest.method();
        // 创建一个查询字符串解码器，用于解析请求 URI 中的查询参数
        QueryStringDecoder qsd = new QueryStringDecoder(httpRequest.uri());
        // 使用路由器对请求进行路由，获取路由结果
        // 注意：此处使用原始类型，会有未检查调用的警告，建议指定泛型参数
        RouteResult<?> routeResult = router.route(method, qsd.path(), qsd.parameters());

        // 检查路由结果是否为 null，若为 null 表示未找到匹配的处理程序
        if (routeResult == null) {
            // 调用 respondNotFound 方法向客户端发送 404 未找到错误响应
            respondNotFound(channelHandlerContext, httpRequest);
            // 处理完毕，直接返回
            return;
        }

        // 若路由结果不为 null，表示找到了匹配的处理程序
        // 调用 routed 方法将请求传递给匹配的处理程序进行处理
        routed(channelHandlerContext, routeResult, httpRequest);
    }

    private void routed(
            ChannelHandlerContext channelHandlerContext,
            RouteResult<?> routeResult,
            HttpRequest httpRequest) {
        ChannelInboundHandler handler = (ChannelInboundHandler) routeResult.target();

        // The handler may have been added (keep alive)
        ChannelPipeline pipeline = channelHandlerContext.pipeline();
        ChannelHandler addedHandler = pipeline.get(ROUTED_HANDLER_NAME);
        if (handler != addedHandler) {
            if (addedHandler == null) {
                pipeline.addAfter(ROUTER_HANDLER_NAME, ROUTED_HANDLER_NAME, handler);
            } else {
                pipeline.replace(addedHandler, ROUTED_HANDLER_NAME, handler);
            }
        }

        RoutedRequest<?> request = new RoutedRequest<>(routeResult, httpRequest);
        channelHandlerContext.fireChannelRead(request.retain());
    }

    private void respondNotFound(ChannelHandlerContext channelHandlerContext, HttpRequest request) {
        LOG.trace(
                "Request could not be routed to any handler. Uri:{} Method:{}",
                request.uri(),
                request.method());
        HandlerUtils.sendErrorResponse(
                channelHandlerContext,
                request,
                new ErrorResponseBody("Not found: " + request.uri()),
                HttpResponseStatus.NOT_FOUND,
                responseHeaders);
    }
}
