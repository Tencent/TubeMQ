/**
 * * Tencent is pleased to support the open source community by making TubeMQ available.
 * <p>
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.corerpc.netty;

import com.google.protobuf.ByteString;
import com.tencent.tubemq.corebase.cluster.NodeAddrInfo;
import com.tencent.tubemq.corebase.protobuf.generated.RPCProtos;
import com.tencent.tubemq.corerpc.RequestWrapper;
import com.tencent.tubemq.corerpc.ResponseWrapper;
import com.tencent.tubemq.corerpc.RpcDataPack;
import com.tencent.tubemq.corerpc.client.CallFuture;
import com.tencent.tubemq.corerpc.client.Callback;
import com.tencent.tubemq.corerpc.client.Client;
import com.tencent.tubemq.corerpc.client.ClientFactory;
import com.tencent.tubemq.corerpc.codec.PbEnDecoder;
import com.tencent.tubemq.corerpc.exception.ClientClosedException;
import com.tencent.tubemq.corerpc.exception.NetworkException;
import com.tencent.tubemq.corerpc.utils.MixUtils;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The network Client for tube rpc service
 */
public class NettyClient implements Client {

    private static final Logger logger =
            LoggerFactory.getLogger(NettyClientHandler.class);
    private static final AtomicLong init = new AtomicLong(0);
    private static Timer timer;
    private final ConcurrentHashMap<Integer, Callback<ResponseWrapper>> requests =
            new ConcurrentHashMap<Integer, Callback<ResponseWrapper>>();
    private final ConcurrentHashMap<Integer, Timeout> timeouts =
            new ConcurrentHashMap<Integer, Timeout>();
    private final AtomicInteger serialNoGenerator =
            new AtomicInteger(0);
    private AtomicBoolean released = new AtomicBoolean(false);
    private NodeAddrInfo addressInfo;
    private ClientFactory clientFactory;
    private Channel channel;
    private long connectTimeout;
    private volatile AtomicBoolean closed = new AtomicBoolean(true);

    /**
     * @param clientFactory
     * @param connectTimeout
     */
    public NettyClient(ClientFactory clientFactory, long connectTimeout) {
        this.clientFactory = clientFactory;
        this.connectTimeout = connectTimeout;
        if (init.incrementAndGet() == 1) {
            timer = new HashedWheelTimer();
        }
    }

    public Channel getChannel() {
        return channel;
    }

    /**
     * @param channel
     * @param addressInfo
     */
    public void setChannel(Channel channel, final NodeAddrInfo addressInfo) {
        this.channel = channel;
        this.addressInfo = addressInfo;
        this.closed.set(false);
    }

    /* (non-Javadoc)
     * @see com.tencent.tubemq.corerpc.client.Client#call(com.tencent.tubemq.corerpc.RequestWrapper,
     *  com.tencent.tubemq.corerpc.client.Callback, long, java.util.concurrent.TimeUnit)
     */
    @Override
    public ResponseWrapper call(RequestWrapper request, Callback callback,
                                long timeout, TimeUnit timeUnit) throws Exception {
        request.setSerialNo(serialNoGenerator.incrementAndGet());
        RPCProtos.RpcConnHeader.Builder builder =
                RPCProtos.RpcConnHeader.newBuilder();
        builder.setFlag(request.getFlagId());
        final RPCProtos.RpcConnHeader connectionHeader =
                builder.build();
        RPCProtos.RequestHeader.Builder headerBuilder =
                RPCProtos.RequestHeader.newBuilder();
        headerBuilder.setServiceType(request.getServiceType());
        headerBuilder.setProtocolVer(request.getProtocolVersion());
        final RPCProtos.RequestHeader rpcHeader =
                headerBuilder.build();
        RPCProtos.RequestBody.Builder rpcBodyBuilder =
                RPCProtos.RequestBody.newBuilder();
        rpcBodyBuilder.setMethod(request.getMethodId());
        rpcBodyBuilder.setTimeout(request.getTimeout());
        rpcBodyBuilder
                .setRequest(ByteString.copyFrom(PbEnDecoder.pbEncode(request.getRequestData())));
        RPCProtos.RequestBody rpcBodyRequest = rpcBodyBuilder.build();
        ByteBufferOutputStream bbo = new ByteBufferOutputStream();
        connectionHeader.writeDelimitedTo(bbo);
        rpcHeader.writeDelimitedTo(bbo);
        rpcBodyRequest.writeDelimitedTo(bbo);
        RpcDataPack pack = new RpcDataPack(request.getSerialNo(), bbo.getBufferList());
        CallFuture<ResponseWrapper> future = new CallFuture<ResponseWrapper>(callback);
        requests.put(request.getSerialNo(), future);
        if (callback == null) {
            try {
                getChannel().write(pack);
                return future.get(timeout, timeUnit);
            } catch (Throwable e) {
                requests.remove(request.getSerialNo());
                throw e;
            }
        } else {
            try {
                timeouts.put(request.getSerialNo(),
                        timer.newTimeout(new TimeoutTask(request.getSerialNo()), timeout, timeUnit));
                //write data after build Timeout to avoid one request processed twice
                getChannel().write(pack);
            } catch (Throwable e) {
                requests.remove(request.getSerialNo());
                throw e;
            }

        }
        return null;
    }


    @Override
    public NodeAddrInfo getServerAddressInfo() {
        return this.addressInfo;
    }

    @Override
    public long getConnectTimeout() {
        return this.connectTimeout;
    }

    @Override
    public boolean isReady() {
        return (!this.closed.get()
                && channel != null
                && channel.isOpen()
                && channel.isBound()
                && channel.isConnected());
    }

    @Override
    public boolean isWritable() {
        return (!this.closed.get()
                && channel != null
                && channel.isWritable());
    }

    @Override
    public void close() {
        close(true);
    }

    /**
     * stop timer
     * remove clientFactory cache
     * handler unfinished callbacks
     * and close the channel
     */
    @Override
    public void close(boolean removeParent) {
        if (this.released.compareAndSet(false, true)) {
            if (init.decrementAndGet() == 0) {
                timer.stop();
            }
        }
        if (this.closed.compareAndSet(false, true)) {
            String clientStr;
            if (this.channel != null) {
                clientStr = channel.toString();
            } else {
                clientStr = this.addressInfo.getHostPortStr();
            }
            if (removeParent) {
                this.clientFactory.removeClient(this.getServerAddressInfo());
            }
            if (!requests.isEmpty()) {
                ClientClosedException exception =
                        new ClientClosedException("Client has bean closed.");
                for (Integer serial : requests.keySet()) {
                    if (serial != null) {
                        Callback<ResponseWrapper> callback = requests.remove(serial);
                        if (callback != null) {
                            callback.handleError(exception);
                        }
                    }
                }
            }
            if (this.channel != null) {
                this.channel.close();
                this.channel = null;
            }
            logger.info(new StringBuilder(256).append("Client(")
                    .append(clientStr).append(") closed").toString());
        }
    }

    @Override
    public ClientFactory getClientFactory() {
        return clientFactory;
    }

    /**
     * tube NettyClientHandler
     */
    public class NettyClientHandler extends SimpleChannelUpstreamHandler {

        @Override
        /**
         * Invoked when a message object (e.g: {@link ChannelBuffer}) was received
         * from a remote peer.
         */
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            if (e.getMessage() instanceof RpcDataPack) {
                RpcDataPack dataPack = (RpcDataPack) e.getMessage();
                Callback callback = requests.remove(dataPack.getSerialNo());
                if (callback != null) {
                    Timeout timeout = timeouts.remove(dataPack.getSerialNo());
                    if (timeout != null) {
                        timeout.cancel();
                    }
                    ResponseWrapper responseWrapper;
                    try {
                        ByteBufferInputStream in = new ByteBufferInputStream(dataPack.getDataLst());
                        RPCProtos.RpcConnHeader connHeader =
                                RPCProtos.RpcConnHeader.parseDelimitedFrom(in);
                        if (connHeader == null) {
                            // When the stream is closed, protobuf doesn't raise an EOFException,
                            // instead, it returns a null message object.
                            throw new EOFException();
                        }
                        RPCProtos.ResponseHeader rpcResponse =
                                RPCProtos.ResponseHeader.parseDelimitedFrom(in);
                        if (rpcResponse == null) {
                            // When the stream is closed, protobuf doesn't raise an EOFException,
                            // instead, it returns a null message object.
                            throw new EOFException();
                        }
                        RPCProtos.ResponseHeader.Status status = rpcResponse.getStatus();
                        if (status == RPCProtos.ResponseHeader.Status.SUCCESS) {
                            RPCProtos.RspResponseBody pbRpcResponse =
                                    RPCProtos.RspResponseBody.parseDelimitedFrom(in);
                            if (pbRpcResponse == null) {
                                // When the RPCProtos parse failed , protobuf doesn't raise an Exception,
                                // instead, it returns a null response object.
                                throw new NetworkException("Not found PBRpcResponse data!");
                            }
                            Object responseResult =
                                    PbEnDecoder.pbDecode(false, pbRpcResponse.getMethod(),
                                            pbRpcResponse.getData().toByteArray());

                            responseWrapper =
                                    new ResponseWrapper(connHeader.getFlag(), dataPack.getSerialNo(),
                                            rpcResponse.getServiceType(), rpcResponse.getProtocolVer(),
                                            pbRpcResponse.getMethod(), responseResult);
                        } else {
                            RPCProtos.RspExceptionBody exceptionResponse =
                                    RPCProtos.RspExceptionBody.parseDelimitedFrom(in);
                            if (exceptionResponse == null) {
                                // When the RPCProtos parse failed , protobuf doesn't raise an Exception,
                                // instead, it returns a null response object.
                                throw new NetworkException("Not found RpcException data!");
                            }
                            responseWrapper =
                                    new ResponseWrapper(connHeader.getFlag(), dataPack.getSerialNo(),
                                            rpcResponse.getServiceType(), rpcResponse.getProtocolVer(),
                                            exceptionResponse.getExceptionName(),
                                            exceptionResponse.getStackTrace());
                        }
                        if (!responseWrapper.isSuccess()) {
                            Throwable remote =
                                    MixUtils.unwrapException(new StringBuilder(512)
                                            .append(responseWrapper.getErrMsg()).append("#")
                                            .append(responseWrapper.getStackTrace()).toString());
                            if (IOException.class.isAssignableFrom(remote.getClass())) {
                                NettyClient.this.close();
                            }
                        }
                        callback.handleResult(responseWrapper);
                    } catch (Throwable ee) {
                        responseWrapper =
                                new ResponseWrapper(-2, dataPack.getSerialNo(), -2, -2, ee);
                        if (ee instanceof EOFException) {
                            NettyClient.this.close();
                        }
                        callback.handleResult(responseWrapper);
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Missing previous call info, maybe it has been timeout.");
                    }
                }
            }
        }

        /**
         * Invoked when an exception was raised by an I/O thread or a
         * {@link ChannelHandler}.
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            Throwable t = e.getCause();
            if (t != null
                    && (t instanceof IOException
                    || t instanceof ReadTimeoutException
                    || t instanceof UnresolvedAddressException)) {
                if (t instanceof ReadTimeoutException) {
                    logger.info("Close client {} due to idle.", e.getChannel());
                }
                if (t instanceof UnresolvedAddressException) {
                    logger
                            .info("UnresolvedAddressException for connect {} closed.", addressInfo.getHostPortStr());
                }
                NettyClient.this.close();
            } else {
                logger.error("catch some exception not IOException", e.getCause());
            }
        }

        @Override
        /**
         * Invoked when a {@link Channel} was closed and all its related resources
         * were released.
         */
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            NettyClient.this.close();
        }
    }

    /**
     * Time out task call back handle
     */
    public class TimeoutTask implements TimerTask {

        private int serialNo;

        public TimeoutTask(int serialNo) {
            this.serialNo = serialNo;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            Timeout timeout1 = timeouts.remove(serialNo);
            if (timeout1 != null) {
                timeout1.cancel();
            }
            final Callback callback = requests.remove(serialNo);
            if (callback != null) {
                channel.getPipeline().execute(new Runnable() {
                    @Override
                    public void run() {
                        callback.handleError(new TimeoutException("Request is timeout!"));
                    }
                });
            }
        }
    }
}
