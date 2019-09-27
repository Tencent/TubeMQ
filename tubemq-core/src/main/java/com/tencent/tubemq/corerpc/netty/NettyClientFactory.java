/*
 * Tencent is pleased to support the open source community by making TubeMQ available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.tubemq.corerpc.netty;

import com.tencent.tubemq.corebase.cluster.NodeAddrInfo;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.client.Client;
import com.tencent.tubemq.corerpc.client.ClientFactory;
import com.tencent.tubemq.corerpc.exception.LocalConnException;
import com.tencent.tubemq.corerpc.utils.TSSLEngineUtil;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLEngine;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Network communication between service processes based on netty
 * see @link MessageSessionFactory Manage network connections
 */
public class NettyClientFactory implements ClientFactory {

    private static final Logger logger =
            LoggerFactory.getLogger(NettyClientFactory.class);
    protected final ConcurrentHashMap<String, Client> clients =
            new ConcurrentHashMap<String, Client>();
    protected AtomicBoolean shutdown = new AtomicBoolean(true);
    private Timer timer = new HashedWheelTimer();
    private volatile AtomicBoolean init = new AtomicBoolean(true);
    private ChannelFactory channelFactory;
    private MemoryAwareThreadPoolExecutor eventExecutor;
    private ExecutorService bossExecutorService;
    private ExecutorService workerExecutorService;
    private AtomicInteger workerIdCounter = new AtomicInteger(0);
    private RpcConfig factoryConf;
    // TSL encryption and need Two Way Authentic
    private boolean enableTLS = false;
    private boolean needTwoWayAuthentic = false;
    private String keyStorePath, keyStorePassword, trustStorePath, trustStorePassword;

    public NettyClientFactory() {

    }


    /**
     * initial the network by rpc config object
     *
     * @param conf
     * @throws IllegalArgumentException
     */
    public void configure(final RpcConfig conf) throws IllegalArgumentException {
        if (this.init.compareAndSet(false, true)) {
            this.timer = new HashedWheelTimer();
        }
        if (this.shutdown.compareAndSet(true, false)) {
            this.factoryConf = conf;
            enableTLS = conf.getBoolean(RpcConstants.TLS_OVER_TCP, false);
            needTwoWayAuthentic = conf.getBoolean(RpcConstants.TLS_TWO_WAY_AUTHENTIC, false);
            if (enableTLS) {
                trustStorePath = conf.getString(RpcConstants.TLS_TRUSTSTORE_PATH);
                trustStorePassword = conf.getString(RpcConstants.TLS_TRUSTSTORE_PASSWORD);
                if (needTwoWayAuthentic) {
                    keyStorePath = conf.getString(RpcConstants.TLS_KEYSTORE_PATH);
                    keyStorePassword = conf.getString(RpcConstants.TLS_KEYSTORE_PASSWORD);
                } else {
                    keyStorePath = null;
                    keyStorePassword = null;
                }
            } else {
                keyStorePath = null;
                keyStorePassword = null;
                trustStorePath = null;
                trustStorePassword = null;
            }
            final int bossCount =
                    conf.getInt(RpcConstants.BOSS_COUNT,
                            RpcConstants.CFG_DEFAULT_BOSS_COUNT);
            final int workerCount =
                    conf.getInt(RpcConstants.WORKER_COUNT,
                            RpcConstants.CFG_DEFAULT_CLIENT_WORKER_COUNT);
            final int callbackCount =
                    conf.getInt(RpcConstants.CALLBACK_WORKER_COUNT, 3);
            bossExecutorService = Executors.newCachedThreadPool();
            workerExecutorService = Executors.newCachedThreadPool();
            this.channelFactory = new NioClientSocketChannelFactory(bossExecutorService, bossCount,
                    new NioWorkerPool(workerExecutorService, workerCount, new ThreadNameDeterminer() {
                        @Override
                        public String determineThreadName(String currentThreadName, String proposedThreadName)
                                throws Exception {
                            return new StringBuilder(256)
                                    .append(conf.getString(RpcConstants.WORKER_THREAD_NAME,
                                            RpcConstants.CFG_DEFAULT_WORKER_THREAD_NAME))
                                    .append(workerIdCounter.incrementAndGet()).toString();
                        }
                    }));
            this.eventExecutor = new MemoryAwareThreadPoolExecutor(
                    callbackCount,
                    conf.getInt(RpcConstants.WORKER_MEM_SIZE,
                            RpcConstants.CFG_DEFAULT_TOTAL_MEM_SIZE),
                    conf.getInt(RpcConstants.WORKER_MEM_SIZE,
                            RpcConstants.CFG_DEFAULT_TOTAL_MEM_SIZE));
        }
    }

    @Override
    public Client getClient(NodeAddrInfo addressInfo, RpcConfig conf) throws Exception {
        Client client = clients.get(addressInfo.getHostPortStr());
        // use the cache network client
        if (client != null && client.isReady()) {
            return client;
        }
        synchronized (this) {
            // check client has been build already
            client = clients.get(addressInfo.getHostPortStr());
            if (client != null && client.isReady()) {
                return client;
            }

            // clean and build a new network client
            if (client != null) {
                client = clients.remove(addressInfo.getHostPortStr());
                if (client != null) {
                    client.close();
                }
                client = null;
            }
            int connectTimeout = conf.getInt(RpcConstants.CONNECT_TIMEOUT, 3000);
            try {
                client = createClient(addressInfo, connectTimeout, conf);
                Client existClient =
                        clients.putIfAbsent(addressInfo.getHostPortStr(), client);
                if (existClient != null) {
                    client.close(false);
                    client = existClient;
                }
            } catch (LocalConnException e) {
                if (client != null) {
                    client.close(false);
                }
                throw e;
            } catch (Exception e) {
                if (client != null) {
                    client.close(false);
                }
                throw e;
            } catch (Throwable ee) {
                if (client != null) {
                    client.close(false);
                }
                throw new Exception(ee);
            }
        }
        return client;
    }

    @Override
    public Client removeClient(NodeAddrInfo addressInfo) {
        return clients.remove(addressInfo.getHostPortStr());
    }

    @Override
    public boolean isShutdown() {
        return this.shutdown.get();
    }

    @Override
    public void shutdown() {
        // stop timer
        if (this.init.compareAndSet(true, false)) {
            timer.stop();
        }
        // shutdown and release network resources
        if (this.shutdown.compareAndSet(false, true)) {
            try {
                if (!clients.isEmpty()) {
                    for (String key : clients.keySet()) {
                        if (key != null) {
                            Client client = clients.remove(key);
                            if (client != null) {
                                client.close();
                            }
                        }
                    }
                }
                if (this.bossExecutorService != null) {
                    this.bossExecutorService.shutdown();
                }
                if (this.workerExecutorService != null) {
                    this.workerExecutorService.shutdown();
                }
                if (this.eventExecutor != null) {
                    this.eventExecutor.shutdown();
                }
            } finally {
                this.channelFactory.releaseExternalResources();
                this.channelFactory.shutdown();
            }
        }
    }

    /**
     * create a netty client
     *
     * @param addressInfo
     * @param connectTimeout
     * @param conf
     * @return
     * @throws Exception
     */
    private Client createClient(final NodeAddrInfo addressInfo,
                                int connectTimeout, final RpcConfig conf) throws Exception {
        final NettyClient client =
                new NettyClient(this, connectTimeout);
        ClientBootstrap clientBootstrap = new ClientBootstrap();
        clientBootstrap.setOption("tcpNoDelay", true);
        clientBootstrap.setOption("reuseAddress", true);
        clientBootstrap.setOption("connectTimeoutMillis", connectTimeout);
        clientBootstrap.setFactory(this.channelFactory);
        long nettyWriteHighMark =
                conf.getLong(RpcConstants.NETTY_WRITE_HIGH_MARK, -1);
        long nettyWriteLowMark =
                conf.getLong(RpcConstants.NETTY_WRITE_LOW_MARK, -1);
        if (nettyWriteHighMark > 0) {
            clientBootstrap.setOption("writeBufferHighWaterMark", nettyWriteHighMark);
        }
        if (nettyWriteLowMark > 0) {
            clientBootstrap.setOption("writeBufferLowWaterMark", nettyWriteLowMark);
        }
        clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                if (enableTLS) {
                    try {
                        SSLEngine sslEngine =
                                TSSLEngineUtil.createSSLEngine(keyStorePath, trustStorePath,
                                        keyStorePassword, trustStorePassword, true, needTwoWayAuthentic);
                        pipeline.addLast("ssl", new SslHandler(sslEngine));
                    } catch (Throwable t) {
                        logger.error(new StringBuilder(256)
                                .append("Create SSLEngine to connection ")
                                .append(addressInfo.getHostPortStr()).append(" failure!").toString(), t);
                        throw new Exception(t);
                    }
                }
                // Encode the data
                pipeline.addLast("protocolEncoder", new NettyProtocolEncoder());
                // Decode the bytes into a Rpc Data Pack
                pipeline.addLast("protocolDecoder", new NettyProtocolDecoder());
                // handle the time out requests
                pipeline.addLast("readTimeoutHandler", new ReadTimeoutHandler(timer,
                        conf.getLong(RpcConstants.CONNECT_READ_IDLE_DURATION,
                                RpcConstants.CFG_CONNECT_READ_IDLE_TIME), TimeUnit.MILLISECONDS));
                // execution handler
                pipeline.addLast("execution", new ExecutionHandler(eventExecutor));
                // tube netty client handler
                pipeline.addLast("clientHandler", client.new NettyClientHandler());
                return pipeline;
            }
        });
        ChannelFuture future =
                clientBootstrap.connect(new InetSocketAddress(addressInfo.getHost(), addressInfo.getPort()));
        future.awaitUninterruptibly(connectTimeout);
        if (!future.isDone()) {
            future.cancel();
            throw new LocalConnException(new StringBuilder(256).append("Create connection to ")
                    .append(addressInfo.getHostPortStr()).append(" timeout!").toString());
        }
        if (future.isCancelled()) {
            throw new LocalConnException(new StringBuilder(256).append("Create connection to ")
                    .append(addressInfo.getHostPortStr()).append(" cancelled by user!").toString());
        }
        if (!future.isSuccess()) {
            throw new LocalConnException(new StringBuilder(256).append("Create connection to ")
                    .append(addressInfo.getHostPortStr()).append(" error").toString(), future.getCause());
        }
        client.setChannel(future.getChannel(), addressInfo);
        return client;
    }


}
