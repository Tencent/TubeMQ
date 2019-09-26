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

package com.tencent.tubemq.corerpc.benchemark;

import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.RpcServiceFactory;
import com.tencent.tubemq.corerpc.netty.NettyClientFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RcpService4BenchmarkClient {

    private static final Logger logger =
            LoggerFactory.getLogger(RcpService4BenchmarkClient.class);

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final String targetHost;
    private final int targetPort;
    private final RpcServiceFactory rpcServiceFactory;
    private final NettyClientFactory clientFactory = new NettyClientFactory();
    private SimpleService simpleService;
    private int threadNum = 10;
    private int invokeTimes = 1000000;

    public RcpService4BenchmarkClient(String targetHost, int targetPort, int threadNum,
                                      int invokeTimes) {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.threadNum = threadNum;
        this.invokeTimes = invokeTimes;
        RpcConfig config = new RpcConfig();
        config.put(RpcConstants.RPC_CODEC, 6);
        config.put(RpcConstants.CONNECT_TIMEOUT, 3000);
        config.put(RpcConstants.REQUEST_TIMEOUT, 10000);

        clientFactory.configure(config);
        rpcServiceFactory = new RpcServiceFactory(clientFactory);
        BrokerInfo brokerInfo = new BrokerInfo(1, targetHost, targetPort);
        this.simpleService =
                rpcServiceFactory.getService(SimpleService.class, brokerInfo, config);
    }

    public static void main(String[] args) throws Exception {
        new RcpService4BenchmarkClient("127.0.0.1", 8088, 10, 100000).start();
    }

    public void start() throws Exception {
        for (int i = 0; i < threadNum; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    for (int j = 0; j < invokeTimes; j++) {
                        simpleService.echo("This is a test.");
                    }
                    System.out.println(Thread.currentThread().getName() + " execute " + invokeTimes);
                    long endTime = System.currentTimeMillis() - startTime;
                    System.out.println("cost time:" + endTime + " ms");
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
}
