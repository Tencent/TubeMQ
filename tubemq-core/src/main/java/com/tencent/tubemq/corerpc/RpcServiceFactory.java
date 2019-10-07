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

package com.tencent.tubemq.corerpc;

import com.tencent.tubemq.corebase.Shutdownable;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.MasterInfo;
import com.tencent.tubemq.corebase.cluster.NodeAddrInfo;
import com.tencent.tubemq.corerpc.client.ClientFactory;
import com.tencent.tubemq.corerpc.exception.LocalConnException;
import com.tencent.tubemq.corerpc.netty.NettyRpcServer;
import com.tencent.tubemq.corerpc.protocol.RpcProtocol;
import com.tencent.tubemq.corerpc.server.ServiceRpcServer;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tube Rpc Service Factory used by all tube service process
 */
public class RpcServiceFactory {

    private static final Logger logger =
            LoggerFactory.getLogger(RpcServiceFactory.class);
    private static final int DEFAULT_IDLE_TIME = 10 * 60 * 1000;
    private static AtomicInteger threadIdGen = new AtomicInteger(0);
    private final ClientFactory clientFactory;
    private final ConcurrentHashMap<Integer, ServiceRpcServer> servers =
            new ConcurrentHashMap<Integer, ServiceRpcServer>();
    private final ConcurrentHashMap<String, ServiceHolder> servicesCache =
            new ConcurrentHashMap<String, ServiceHolder>();
    private final ConcurrentHashMap<String/* addr */, RemoteConErrStats> remoteAddrMap =
            new ConcurrentHashMap<String, RemoteConErrStats>();
    private final ConcurrentHashMap<String/* addr */, Long> forbiddenAddrMap =
            new ConcurrentHashMap<String, Long>();
    private final ConnectionManager connectionManager;
    private final ConcurrentHashMap<String, ConnectionNode> brokerQueue =
            new ConcurrentHashMap<String, ConnectionNode>();
    private final ConcurrentHashMap<String, Long> updateTime =
            new ConcurrentHashMap<String, Long>();
    private AtomicLong lastLogPrintTime = new AtomicLong(0);
    private AtomicLong lastCheckTime = new AtomicLong(0);
    private long linkStatsDurationMs =
            RpcConstants.CFG_LQ_STATS_DURATION_MS;
    private long linkStatsForbiddenDurMs =
            RpcConstants.CFG_LQ_FORBIDDEN_DURATION_MS;
    private int linkStatsMaxAllowedFailCount =
            RpcConstants.CFG_LQ_MAX_ALLOWED_FAIL_COUNT;
    private double linkStatsMaxAllowedForbiddenRate =
            RpcConstants.CFG_LQ_MAX_FAIL_FORBIDDEN_RATE;

    /**
     * initial an empty server factory
     */
    public RpcServiceFactory() {
        this.clientFactory = null;
        this.connectionManager = null;
    }

    /**
     * initial with an tube clientFactory
     */
    public RpcServiceFactory(final ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        this.connectionManager = null;
    }

    /**
     * initial with an tube clientFactory and rpc config
     */
    public RpcServiceFactory(final ClientFactory clientFactory, final RpcConfig config) {
        this.clientFactory = clientFactory;
        this.linkStatsDurationMs =
                config.getLong(RpcConstants.RPC_LQ_STATS_DURATION,
                        RpcConstants.CFG_LQ_STATS_DURATION_MS);
        this.linkStatsForbiddenDurMs =
                config.getLong(RpcConstants.RPC_LQ_FORBIDDEN_DURATION,
                        RpcConstants.CFG_LQ_FORBIDDEN_DURATION_MS);
        this.linkStatsMaxAllowedFailCount =
                config.getInt(RpcConstants.RPC_LQ_MAX_ALLOWED_FAIL_COUNT,
                        RpcConstants.CFG_LQ_MAX_ALLOWED_FAIL_COUNT);
        this.linkStatsMaxAllowedForbiddenRate =
                config.getDouble(RpcConstants.RPC_LQ_MAX_FAIL_FORBIDDEN_RATE,
                        RpcConstants.CFG_LQ_MAX_FAIL_FORBIDDEN_RATE);
        connectionManager = new ConnectionManager();
        connectionManager.setName(new StringBuilder(256)
                .append("rpcFactory-Thread-")
                .append(threadIdGen.getAndIncrement()).toString());
        connectionManager.start();
    }

    /**
     * check if the remote address is forbidden or not
     *
     * @param remoteAddr
     * @return
     */
    public boolean isRemoteAddrForbidden(String remoteAddr) {
        Long forbiddenTime = forbiddenAddrMap.get(remoteAddr);
        if (forbiddenTime == null) {
            return false;
        }
        if ((System.currentTimeMillis() - forbiddenTime) <= linkStatsForbiddenDurMs) {
            return true;
        }
        // Forbidden time out then remove it
        forbiddenAddrMap.remove(remoteAddr);
        return false;
    }

    /**
     * get all Forbidden Address
     *
     * @return
     */
    public ConcurrentHashMap<String, Long> getForbiddenAddrMap() {
        return forbiddenAddrMap;
    }

    /**
     * @param remoteAddr
     */
    public void resetRmtAddrErrCount(String remoteAddr) {
        forbiddenAddrMap.remove(remoteAddr);
        RemoteConErrStats rmtConErrStats =
                remoteAddrMap.get(remoteAddr);
        if (rmtConErrStats == null) {
            RemoteConErrStats newErrStatistic =
                    new RemoteConErrStats(linkStatsDurationMs, linkStatsMaxAllowedFailCount);
            rmtConErrStats = remoteAddrMap.putIfAbsent(remoteAddr, newErrStatistic);
            if (rmtConErrStats == null) {
                rmtConErrStats = newErrStatistic;
            }
        }
        rmtConErrStats.resetErrCount();
    }

    /**
     * @param remoteAddr
     */
    public void addRmtAddrErrCount(String remoteAddr) {
        RemoteConErrStats rmtConErrStats = remoteAddrMap.get(remoteAddr);
        if (rmtConErrStats == null) {
            RemoteConErrStats newErrStatistic =
                    new RemoteConErrStats(linkStatsDurationMs, linkStatsMaxAllowedFailCount);
            rmtConErrStats = remoteAddrMap.putIfAbsent(remoteAddr, newErrStatistic);
            if (rmtConErrStats == null) {
                rmtConErrStats = newErrStatistic;
            }
        }
        if (rmtConErrStats.increErrCount()) {
            boolean isAdded = false;
            Long beforeTime = forbiddenAddrMap.get(remoteAddr);
            if (beforeTime == null) {
                int totalCount = 0;
                Long curTime = System.currentTimeMillis();
                Set<String> expiredAddrs = new HashSet<String>();
                for (Map.Entry<String, Long> entry : forbiddenAddrMap.entrySet()) {
                    if (entry.getKey() == null || entry.getValue() == null) {
                        continue;
                    }
                    if ((curTime - entry.getValue()) > linkStatsForbiddenDurMs) {
                        expiredAddrs.add(entry.getKey());
                        continue;
                    }
                    totalCount++;
                }
                if (!expiredAddrs.isEmpty()) {
                    for (String tmpAddr : expiredAddrs) {
                        Long sotreTime = forbiddenAddrMap.get(tmpAddr);
                        if (sotreTime == null) {
                            continue;
                        }
                        if ((curTime - sotreTime) > linkStatsForbiddenDurMs) {
                            forbiddenAddrMap.remove(tmpAddr);
                        }
                    }
                }
                int needForbiddenCount =
                        (int) Math.rint(remoteAddrMap.size() * linkStatsMaxAllowedForbiddenRate);
                needForbiddenCount = (needForbiddenCount > 30) ? 30 : needForbiddenCount;
                if (needForbiddenCount > totalCount) {
                    forbiddenAddrMap.put(remoteAddr, System.currentTimeMillis());
                    isAdded = true;
                }
            } else {
                forbiddenAddrMap.put(remoteAddr, System.currentTimeMillis());
                isAdded = true;
            }
            long curLastPrintTime = lastLogPrintTime.get();
            if ((isAdded)
                    && (System.currentTimeMillis() - curLastPrintTime > 120000)) {
                if (lastLogPrintTime.compareAndSet(curLastPrintTime, System.currentTimeMillis())) {
                    logger.info(new StringBuilder(512)
                            .append("[Remote Address] forbidden list : ")
                            .append(forbiddenAddrMap.toString()).toString());
                }
            }
        }
    }

    public void rmvAllExpiredRecords() {
        long curTime = System.currentTimeMillis();
        Set<String> expiredAddrs = new HashSet<String>();
        for (Map.Entry<String, RemoteConErrStats> entry : remoteAddrMap.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (entry.getValue().isExpiredRecord(curTime)) {
                expiredAddrs.add(entry.getKey());
            }
        }
        if (!expiredAddrs.isEmpty()) {
            for (String tmpAddr : expiredAddrs) {
                RemoteConErrStats rmtConErrStats =
                        remoteAddrMap.get(tmpAddr);
                if (rmtConErrStats == null) {
                    continue;
                }
                if (rmtConErrStats.isExpiredRecord(curTime)) {
                    remoteAddrMap.remove(tmpAddr);
                }
            }
        }
        expiredAddrs.clear();
        curTime = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : forbiddenAddrMap.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if ((curTime - entry.getValue()) > (linkStatsForbiddenDurMs + 60000)) {
                expiredAddrs.add(entry.getKey());
            }
        }
        if (!expiredAddrs.isEmpty()) {
            for (String tmpAddr : expiredAddrs) {
                Long recordTime = forbiddenAddrMap.get(tmpAddr);
                if (recordTime == null) {
                    continue;
                }
                if ((curTime - recordTime) > (linkStatsForbiddenDurMs + 60000)) {
                    forbiddenAddrMap.remove(tmpAddr);
                }
            }
        }
    }

    /**
     * @param clazz
     * @param brokerInfo
     * @param config
     * @return
     */
    public synchronized <T> T getService(Class<T> clazz,
                                         BrokerInfo brokerInfo,
                                         RpcConfig config) {
        String serviceKey = getServiceKey(brokerInfo.getBrokerAddr(), clazz.getName());
        ServiceHolder h = servicesCache.get(serviceKey);
        if (h != null) {
            updateTime.put(serviceKey, System.currentTimeMillis());
            return (T) h.getService();
        }
        RpcServiceInvoker invoker =
                new RpcServiceInvoker(clientFactory, clazz, config,
                        new NodeAddrInfo(brokerInfo.getHost(), brokerInfo.getPort(), brokerInfo.getBrokerAddr()));
        Object service =
                Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, invoker);
        servicesCache.put(serviceKey, new ServiceHolder(service, invoker));
        updateTime.put(serviceKey, System.currentTimeMillis());
        return (T) service;
    }

    /**
     * check is service empty
     *
     * @return
     */
    public boolean isServiceEmpty() {
        return servicesCache.isEmpty();
    }

    public <T> T getOrCreateService(Class<T> clazz,
                                    BrokerInfo brokerInfo,
                                    RpcConfig config) {
        String serviceKey = getServiceKey(brokerInfo.getBrokerAddr(), clazz.getName());
        ServiceHolder h = servicesCache.get(serviceKey);
        if (h != null) {
            return (T) h.getService();
        } else {
            if (!isRemoteAddrForbidden(brokerInfo.getBrokerAddr())) {
                ConnectionNode curNode = brokerQueue.get(serviceKey);
                if (curNode == null) {
                    brokerQueue.putIfAbsent(serviceKey,
                            new ConnectionNode(clazz, new NodeAddrInfo(brokerInfo.getHost(),
                                    brokerInfo.getPort(), brokerInfo.getBrokerAddr()), config));
                }
            }
            return null;
        }
    }

    public synchronized <T> T getFailoverService(Class<T> clazz,
                                                 MasterInfo masterInfo,
                                                 RpcConfig config) {
        String serviceKey = getFailoverServiceKey(masterInfo, clazz.getName());
        ServiceHolder h = servicesCache.get(serviceKey);
        if (h != null) {
            return (T) h.getService();
        }
        RpcServiceFailoverInvoker invoker =
                new RpcServiceFailoverInvoker(clientFactory, clazz, config, masterInfo);
        Object service =
                Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, invoker);
        servicesCache.put(serviceKey, new ServiceHolder(service, invoker));
        return (T) service;
    }

    public synchronized void destroyAllService() {
        for (String serviceKey : servicesCache.keySet()) {
            if (serviceKey == null) {
                continue;
            }
            ServiceHolder h = servicesCache.remove(serviceKey);
            if (h != null) {
                h.shutdown();
            }
        }
    }

    public synchronized void publishService(Class clazz, Object serviceInstance,
                                            int listenPort, RpcConfig config) throws Exception {
        publishService(clazz, serviceInstance, listenPort, null, config);
    }

    /**
     * start an tube netty server
     *
     * @param clazz
     * @param serviceInstance
     * @param listenPort
     * @param threadPool
     * @param config
     * @throws Exception
     */
    public synchronized void publishService(Class clazz, Object serviceInstance,
                                            int listenPort, ExecutorService threadPool,
                                            RpcConfig config) throws Exception {
        ServiceRpcServer server = servers.get(listenPort);
        if (server == null) {
            server = new NettyRpcServer(config);
            server.start(listenPort);
            servers.put(listenPort, server);
        }
        try {
            server.publishService(clazz.getName(), serviceInstance, threadPool);
        } catch (Exception e) {
            logger.error("Publish service failed!", e);
            throw e;
        }
    }

    public synchronized void destroyAllPublishedService() throws Exception {
        for (Integer serverId : servers.keySet()) {
            if (serverId == null) {
                continue;
            }
            ServiceRpcServer serviceServer = servers.remove(serverId);
            if (serviceServer == null) {
                continue;
            }
            serviceServer.removeAllService(RpcProtocol.RPC_PROTOCOL_TCP);
            serviceServer.removeAllService(RpcProtocol.RPC_PROTOCOL_TLS);
            serviceServer.stop();
        }
    }

    /**
     * shutdown and recycle the connection resources
     *
     * @throws Exception
     */
    public void destroy() throws Exception {
        if (connectionManager != null) {
            connectionManager.shutdown();
        }
        destroyAllService();
        destroyAllPublishedService();
    }

    private String getServiceKey(String targetAddress, String serviceName) {
        return new StringBuilder(256).append(serviceName)
                .append("@").append(targetAddress).toString();
    }

    private String getFailoverServiceKey(MasterInfo masterInfo, String serviceName) {
        return new StringBuilder(256).append(serviceName)
                .append(TokenConstants.GROUP_SEP)
                .append(masterInfo.getMasterClusterStr()).toString();
    }

    private class ServiceHolder<T> implements Shutdownable {
        private T service;
        private AbstractServiceInvoker invoker;

        ServiceHolder(T service, AbstractServiceInvoker invoker) {
            this.service = service;
            this.invoker = invoker;
        }

        public T getService() {
            return service;
        }

        @Override
        public void shutdown() {
            invoker.destroy();
        }
    }

    private class ConnectionNode {
        private Class clazzType;
        private NodeAddrInfo addressInfo;
        private RpcConfig config;


        public ConnectionNode(Class clazzType,
                              NodeAddrInfo nodeAddrInfo,
                              RpcConfig config) {
            this.clazzType = clazzType;
            this.addressInfo = nodeAddrInfo;
            this.config = config;
        }

        public Class getClazzType() {
            return clazzType;
        }

        public NodeAddrInfo getAddressInfo() {
            return addressInfo;
        }

        public String getHostAndPortStr() {
            return addressInfo.getHostPortStr();
        }

        public RpcConfig getConfig() {
            return config;
        }
    }

    /**
     * ConnectionManager
     * Manage and recycle network connection resources
     */
    private class ConnectionManager extends Thread {
        boolean isRunning = true;

        public void shutdown() {
            logger.info("[SHUTDOWN_TUBE] Shutting down connectionManager.");
            isRunning = false;
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    while (!brokerQueue.isEmpty()) {
                        for (String serviceKey : brokerQueue.keySet()) {
                            ConnectionNode node = brokerQueue.get(serviceKey);
                            ServiceHolder h = servicesCache.get(serviceKey);
                            if (h != null) {
                                brokerQueue.remove(serviceKey);
                                continue;
                            }
                            RpcServiceInvoker invoker =
                                    new RpcServiceInvoker(clientFactory, node.clazzType,
                                            node.getConfig(), node.getAddressInfo());
                            Object service =
                                    Proxy.newProxyInstance(node.clazzType.getClassLoader(),
                                            new Class[]{node.clazzType}, invoker);
                            try {
                                invoker.getClientOnce();
                                resetRmtAddrErrCount(node.getHostAndPortStr());
                            } catch (Throwable e) {
                                if (e instanceof LocalConnException) {
                                    addRmtAddrErrCount(node.getHostAndPortStr());
                                }
                                brokerQueue.remove(serviceKey);
                                continue;
                            }
                            servicesCache.putIfAbsent(serviceKey, new ServiceHolder(service, invoker));
                            updateTime.put(serviceKey, System.currentTimeMillis());
                            brokerQueue.remove(serviceKey);
                        }
                    }
                    long cur = System.currentTimeMillis();
                    if (cur - lastCheckTime.get() >= 30000) {
                        ArrayList<String> tmpKeyList = new ArrayList<String>();
                        for (Map.Entry<String, Long> entry : updateTime.entrySet()) {
                            if (entry.getKey() == null || entry.getValue() == null) {
                                continue;
                            }
                            if (cur - entry.getValue() > DEFAULT_IDLE_TIME) {
                                tmpKeyList.add(entry.getKey());
                            }
                        }
                        for (String serviceKey : tmpKeyList) {
                            servicesCache.remove(serviceKey);
                            updateTime.remove(serviceKey);
                        }
                        rmvAllExpiredRecords();
                        lastCheckTime.set(System.currentTimeMillis());
                    }
                } catch (Throwable e2) {
                    logger.warn("[connectionManager]: runner found throw error info ", e2);
                }
                try {
                    Thread.sleep(80);
                } catch (Exception e) {
                    //
                }
            }
        }
    }

}
