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

import com.tencent.tubemq.corebase.cluster.MasterInfo;
import com.tencent.tubemq.corebase.cluster.NodeAddrInfo;
import com.tencent.tubemq.corerpc.client.Callback;
import com.tencent.tubemq.corerpc.client.Client;
import com.tencent.tubemq.corerpc.client.ClientFactory;
import com.tencent.tubemq.corerpc.codec.PbEnDecoder;
import com.tencent.tubemq.corerpc.exception.StandbyException;
import com.tencent.tubemq.corerpc.protocol.RpcProtocol;
import com.tencent.tubemq.corerpc.utils.MixUtils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class RpcServiceFailoverInvoker extends AbstractServiceInvoker {

    private final AtomicInteger retryCounter = new AtomicInteger(0);
    private MasterInfo masterInfo;
    private Client currentClient;
    private int masterNodeCnt;


    public RpcServiceFailoverInvoker(ClientFactory clientFactory, Class serviceClass,
                                     RpcConfig conf, MasterInfo masterInfo) {
        super(clientFactory, serviceClass, conf);
        this.masterInfo = masterInfo;
        this.masterNodeCnt = masterInfo.getNodeHostPortList().size();
        getNextClient();
    }

    @Override
    public Object callMethod(String targetInterface, String method,
                             Object arg, Callback callback) throws Throwable {
        if (currentClient == null
                || !currentClient.isReady()) {
            getNextClient();
        }
        long currentCounter = retryCounter.get();
        RequestWrapper requestWrapper =
                new RequestWrapper(PbEnDecoder.getServiceIdByServiceName(targetInterface),
                        RpcProtocol.RPC_PROTOCOL_VERSION,
                        RpcConstants.RPC_FLAG_MSG_TYPE_REQUEST,
                        requestTimeout);
        requestWrapper.setMethodId(PbEnDecoder.getMethIdByName(method));
        requestWrapper.setRequestData(arg);
        Throwable t = null;
        for (int i = 0; i < masterNodeCnt; i++) {
            if (currentClient != null) {
                ResponseWrapper responseWrapper =
                        currentClient.call(requestWrapper, callback,
                                requestTimeout, TimeUnit.MILLISECONDS);
                if (responseWrapper != null) {
                    if (responseWrapper.isSuccess()) {
                        return responseWrapper.getResponseData();
                    } else {
                        Throwable remote =
                                MixUtils.unwrapException(new StringBuilder(512)
                                        .append(responseWrapper.getErrMsg()).append("#")
                                        .append(responseWrapper.getStackTrace()).toString());
                        if ((IOException.class.isAssignableFrom(remote.getClass()))
                                || (StandbyException.class.isAssignableFrom(remote.getClass()))) {
                            if (currentCounter == retryCounter.get()) {
                                getNextClient();
                                currentCounter++;
                            }
                            t = remote;
                        } else {
                            throw remote;
                        }
                    }
                } else {
                    break;
                }
            } else {
                int index = (int) (currentCounter % (long) (masterNodeCnt));
                t = new IOException(new StringBuilder(512).append("Connect server ")
                        .append(masterInfo.getNodeHostPortList().get(index)).append(" failure!").toString());
                if (currentCounter == retryCounter.get()) {
                    getNextClient();
                    currentCounter++;
                }
            }
        }
        if (t != null) {
            throw t;
        }
        return null;
    }

    private synchronized Client getNextClient() {
        if (currentClient == null || !currentClient.isReady()) {
            Client client = null;
            int retryTimes = masterNodeCnt;
            List<String> addressList = masterInfo.getNodeHostPortList();
            while (client == null || !client.isReady()) {
                String nodeKey =
                        addressList.get((retryCounter.getAndIncrement() & Integer.MAX_VALUE) % masterNodeCnt);
                NodeAddrInfo nodeAddrInfo = masterInfo.getAddrMap4failover().get(nodeKey);
                try {
                    client = clientFactory.getClient(nodeAddrInfo, conf);
                } catch (Throwable e) {
                    //
                }
                if (retryTimes-- == 0) {
                    break;
                }
            }
            if (client != null) {
                currentClient = client;
            }
            return client;
        } else {
            return currentClient;
        }
    }
}
