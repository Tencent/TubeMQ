/*
 * Tencent is pleased to support the open source community by making TubeMQ available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
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

package com.tencent.tubemq.corerpc.netty;

import static com.tencent.tubemq.corebase.utils.AddressUtils.getRemoteAddressIP;
import com.google.protobuf.Message;
import com.tencent.tubemq.corebase.protobuf.generated.RPCProtos;
import com.tencent.tubemq.corerpc.RequestWrapper;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.RpcDataPack;
import com.tencent.tubemq.corerpc.codec.PbEnDecoder;
import com.tencent.tubemq.corerpc.exception.ServerNotReadyException;
import com.tencent.tubemq.corerpc.protocol.Protocol;
import com.tencent.tubemq.corerpc.protocol.ProtocolFactory;
import com.tencent.tubemq.corerpc.protocol.RpcProtocol;
import com.tencent.tubemq.corerpc.server.RequestContext;
import com.tencent.tubemq.corerpc.server.ServiceRpcServer;
import com.tencent.tubemq.corerpc.utils.TSSLEngineUtil;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLEngine;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Netty Rpc Server
 */
public class NettyRpcServer implements ServiceRpcServer {

    private static final Logger logger =
            LoggerFactory.getLogger(NettyRpcServer.class);
    private static final ConcurrentHashMap<String, AtomicLong> errParseAddrMap =
            new ConcurrentHashMap<String, AtomicLong>();
    private static AtomicLong lastParseTime = new AtomicLong(0);
    private final ConcurrentHashMap<Integer, Protocol> protocols =
            new ConcurrentHashMap<Integer, Protocol>();
    private ServerBootstrap bootstrap = null;
    private NioServerSocketChannelFactory channelFactory = null;
    private AtomicBoolean started = new AtomicBoolean(false);
    private int protocolType = RpcProtocol.RPC_PROTOCOL_TCP;
    private boolean isOverTLS = false;
    private String keyStorePath = "";
    private String keyStorePassword = "";
    private boolean needTwoWayAuthentic = false;
    private String trustStorePath = "";
    private String trustStorePassword = "";

    /**
     * create a server with rpc config info
     *
     * @param conf
     * @throws Exception
     */
    public NettyRpcServer(RpcConfig conf) throws Exception {
        this.isOverTLS = conf.getBoolean(RpcConstants.TLS_OVER_TCP, false);
        if (this.isOverTLS) {
            this.protocolType = RpcProtocol.RPC_PROTOCOL_TLS;
            this.keyStorePath = conf.getString(RpcConstants.TLS_KEYSTORE_PATH);
            this.keyStorePassword = conf.getString(RpcConstants.TLS_KEYSTORE_PASSWORD);
            this.needTwoWayAuthentic = conf.getBoolean(RpcConstants.TLS_TWO_WAY_AUTHENTIC, false);
            if (this.needTwoWayAuthentic) {
                this.trustStorePath = conf.getString(RpcConstants.TLS_TRUSTSTORE_PATH);
                this.trustStorePassword = conf.getString(RpcConstants.TLS_TRUSTSTORE_PASSWORD);
            }
            if (keyStorePath == null || keyStorePassword == null) {
                throw new Exception(new StringBuilder(512).append("Required parameters: ")
                        .append(RpcConstants.TLS_KEYSTORE_PATH).append(" or ")
                        .append(RpcConstants.TLS_KEYSTORE_PASSWORD).append(" for TLS!").toString());
            }
            if (this.needTwoWayAuthentic) {
                if (trustStorePath == null || trustStorePassword == null) {
                    throw new Exception(new StringBuilder(512).append("Required parameters: ")
                            .append(RpcConstants.TLS_TRUSTSTORE_PATH).append(" or ")
                            .append(RpcConstants.TLS_TRUSTSTORE_PASSWORD).append(" for TLS!").toString());
                }
            }
        }
        int bossCount =
                conf.getInt(RpcConstants.BOSS_COUNT,
                        RpcConstants.CFG_DEFAULT_BOSS_COUNT);
        int workerCount =
                conf.getInt(RpcConstants.WORKER_COUNT,
                        RpcConstants.CFG_DEFAULT_SERVER_WORKER_COUNT);
        this.bootstrap =
                new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                        bossCount, Executors.newCachedThreadPool(), workerCount));
        bootstrap.setOption("tcpNoDelay",
                conf.getBoolean(RpcConstants.TCP_NODELAY, true));
        bootstrap.setOption("reuseAddress",
                conf.getBoolean(RpcConstants.TCP_REUSEADDRESS, true));
        long nettyWriteHighMark =
                conf.getLong(RpcConstants.NETTY_WRITE_HIGH_MARK, -1);
        if (nettyWriteHighMark > 0) {
            bootstrap.setOption("writeBufferHighWaterMark", nettyWriteHighMark);
        }
        long nettyWriteLowMark =
                conf.getLong(RpcConstants.NETTY_WRITE_LOW_MARK, -1);
        if (nettyWriteLowMark > 0) {
            bootstrap.setOption("writeBufferLowWaterMark", nettyWriteLowMark);
        }
        long nettySendBuf = conf.getLong(RpcConstants.NETTY_TCP_SENDBUF, -1);
        if (nettySendBuf > 0) {
            bootstrap.setOption("sendBufferSize", nettySendBuf);
        }
        long nettyRecvBuf = conf.getLong(RpcConstants.NETTY_TCP_RECEIVEBUF, -1);
        if (nettyRecvBuf > 0) {
            bootstrap.setOption("receiveBufferSize", nettyRecvBuf);
        }
    }

    @Override
    public void start(int listenPort) throws Exception {
        if (this.started.get()) {
            return;
        }
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = new DefaultChannelPipeline();
                if (isOverTLS) {
                    try {
                        SSLEngine sslEngine =
                                TSSLEngineUtil.createSSLEngine(keyStorePath, trustStorePath,
                                        keyStorePassword, trustStorePassword, false, needTwoWayAuthentic);
                        pipeline.addLast("ssl", new SslHandler(sslEngine));
                    } catch (Throwable t) {
                        logger.error(
                                "TLS NettyRpcServer init SSLEngine error, system auto exit!", t);
                        System.exit(1);
                    }
                }
                // Encode the data handler
                pipeline.addLast("protocolEncoder", new NettyProtocolDecoder());
                // Decode the bytes into a Rpc Data Pack
                pipeline.addLast("protocolDecoder", new NettyProtocolEncoder());
                // tube netty Server handler
                pipeline.addLast("serverHandler", new NettyServerHandler(protocolType));
                return pipeline;
            }
        });
        bootstrap.bind(new InetSocketAddress(listenPort));
        this.started.set(true);
        if (isOverTLS) {
            logger.info(new StringBuilder(256)
                    .append("TLS RpcServer started, listen port: ")
                    .append(listenPort).toString());
        } else {
            logger.info(new StringBuilder(256)
                    .append("TCP RpcServer started, listen port: ")
                    .append(listenPort).toString());
        }
    }

    @Override
    public void publishService(String serviceName, Object serviceInstance,
                               ExecutorService threadPool) throws Exception {
        Protocol protocol = protocols.get(protocolType);
        if (protocol == null) {
            if (ProtocolFactory.getProtocol(protocolType) == null) {
                throw new Exception(new StringBuilder(256)
                        .append("Invalid protocol type ").append(protocolType)
                        .append("! You have to register you new protocol before publish service.").toString());
            }
            protocol = ProtocolFactory.getProtocolInstance(protocolType);
            protocols.put(protocolType, protocol);
        }
        protocol.registerService(isOverTLS, serviceName, serviceInstance, threadPool);
    }

    @Override
    public void removeService(int protocolType, String serviceName) throws Exception {
        Protocol protocol = protocols.get(protocolType);
        if (protocol != null) {
            protocol.removeService(serviceName);
        }
    }

    @Override
    public void removeAllService(int protocolType) throws Exception {
        Protocol protocol = protocols.get(protocolType);
        if (protocol != null) {
            protocol.removeAllService();
        }
    }

    @Override
    public boolean isServiceStarted() {
        return this.started.get();
    }

    @Override
    public void stop() throws Exception {
        if (!this.started.get()) {
            return;
        }
        if (this.started.compareAndSet(true, false)) {
            logger.info("Stopping RpcServer...");
            bootstrap.releaseExternalResources();
            logger.info("RpcServer stop successfully.");
        }
    }

    /**
     * Netty Server Handler
     */
    private class NettyServerHandler extends SimpleChannelUpstreamHandler {

        private int protocolType = RpcProtocol.RPC_PROTOCOL_TCP;


        public NettyServerHandler(int protocolType) {
            this.protocolType = protocolType;
        }

        /**
         * Invoked when an exception was raised by an I/O thread or a
         * {@link ChannelHandler}.
         */
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            if (!(e.getCause() instanceof IOException)) {
                logger.error("catch some exception not IOException", e.getCause());
            }
        }

        /**
         * Invoked when a message object (e.g: {@link ChannelBuffer}) was received
         * from a remote peer.
         */
        public void messageReceived(final ChannelHandlerContext ctx,
                                    MessageEvent e) throws Exception {
            if (!(e.getMessage() instanceof RpcDataPack)) {
                return;
            }
            RpcDataPack dataPack = (RpcDataPack) e.getMessage();
            RPCProtos.RpcConnHeader connHeader;
            RPCProtos.RequestHeader requestHeader;
            RPCProtos.RequestBody rpcRequestBody;
            Channel channel = ctx.getChannel();
            if (channel == null) {
                return;
            }
            String rmtaddrIp = getRemoteAddressIP(channel);
            try {
                if (!isServiceStarted()) {
                    throw new ServerNotReadyException("RpcServer is not running yet");
                }
                List<ByteBuffer> req = dataPack.getDataLst();
                ByteBufferInputStream dis = new ByteBufferInputStream(req);
                connHeader = RPCProtos.RpcConnHeader.parseDelimitedFrom(dis);
                requestHeader = RPCProtos.RequestHeader.parseDelimitedFrom(dis);
                rpcRequestBody = RPCProtos.RequestBody.parseDelimitedFrom(dis);
            } catch (Throwable e1) {
                if (!(e1 instanceof ServerNotReadyException)) {
                    if (rmtaddrIp != null) {
                        AtomicLong count = errParseAddrMap.get(rmtaddrIp);
                        if (count == null) {
                            AtomicLong tmpCount = new AtomicLong(0);
                            count = errParseAddrMap.putIfAbsent(rmtaddrIp, tmpCount);
                            if (count == null) {
                                count = tmpCount;
                            }
                        }
                        count.incrementAndGet();
                        long befTime = lastParseTime.get();
                        long curTime = System.currentTimeMillis();
                        if (curTime - befTime > 180000) {
                            if (lastParseTime.compareAndSet(befTime, System.currentTimeMillis())) {
                                logger.warn(new StringBuilder(512)
                                        .append("[Abnormal Visit] Abnormal Message Content visit list is :")
                                        .append(errParseAddrMap).toString());
                                errParseAddrMap.clear();
                            }
                        }
                    }
                }
                List<ByteBuffer> res =
                        prepareResponse(null, RPCProtos.ResponseHeader.Status.FATAL,
                                e1.getClass().getName(), new StringBuilder(512)
                                        .append("IPC server unable to read call parameters:")
                                        .append(e1.getMessage()).toString());
                if (res != null) {
                    dataPack.setDataLst(res);
                    channel.write(dataPack);
                }
                return;
            }
            try {
                RequestWrapper requestWrapper =
                        new RequestWrapper(requestHeader.getServiceType(),
                                this.protocolType, requestHeader.getProtocolVer(),
                                connHeader.getFlag(), rpcRequestBody.getTimeout());
                requestWrapper.setMethodId(rpcRequestBody.getMethod());
                requestWrapper.setRequestData(PbEnDecoder.pbDecode(true,
                        rpcRequestBody.getMethod(), rpcRequestBody.getRequest().toByteArray()));
                requestWrapper.setSerialNo(dataPack.getSerialNo());
                RequestContext context =
                        new NettyRequestContext(requestWrapper, ctx, System.currentTimeMillis());
                protocols.get(this.protocolType).handleRequest(context, rmtaddrIp);
            } catch (Throwable ee) {
                List<ByteBuffer> res =
                        prepareResponse(null, RPCProtos.ResponseHeader.Status.FATAL,
                                ee.getClass().getName(), new StringBuilder(512)
                                        .append("IPC server handle request error :")
                                        .append(ee.getMessage()).toString());
                if (res != null) {
                    dataPack.setDataLst(res);
                    ctx.getChannel().write(dataPack);
                }
                return;
            }
        }

        /**
         * prepare and write the message into an list of byte buffers
         *
         * @param value
         * @param status
         * @param errorClass
         * @param error
         * @return
         */
        protected List<ByteBuffer> prepareResponse(Object value,
                                                   RPCProtos.ResponseHeader.Status status,
                                                   String errorClass, String error) {
            ByteBufferOutputStream buf = new ByteBufferOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            try {
                RPCProtos.RpcConnHeader.Builder connBuilder =
                        RPCProtos.RpcConnHeader.newBuilder();
                connBuilder.setFlag(RpcConstants.RPC_FLAG_MSG_TYPE_RESPONSE);
                connBuilder.build().writeDelimitedTo(out);
                RPCProtos.ResponseHeader.Builder builder =
                        RPCProtos.ResponseHeader.newBuilder();
                builder.setStatus(status);
                builder.build().writeDelimitedTo(out);
                if (error != null) {
                    RPCProtos.RspExceptionBody.Builder b =
                            RPCProtos.RspExceptionBody.newBuilder();
                    b.setExceptionName(errorClass);
                    b.setStackTrace(error);
                    b.build().writeDelimitedTo(out);
                } else {
                    if (value != null) {
                        ((Message) value).writeDelimitedTo(out);
                    }
                }
            } catch (IOException e) {
                logger.warn(new StringBuilder(512)
                        .append("Exception while creating response ")
                        .append(e).toString());
            }
            return buf.getBufferList();
        }
    }
}
