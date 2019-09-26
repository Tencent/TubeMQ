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

package com.tencent.tubemq.corerpc.client;

import com.tencent.tubemq.corebase.cluster.NodeAddrInfo;
import com.tencent.tubemq.corerpc.RequestWrapper;
import com.tencent.tubemq.corerpc.ResponseWrapper;
import java.util.concurrent.TimeUnit;


public interface Client {

    ResponseWrapper call(RequestWrapper request, Callback callback,
                         long timeout, TimeUnit timeUnit) throws Exception;

    NodeAddrInfo getServerAddressInfo();

    long getConnectTimeout();

    boolean isReady();

    boolean isWritable();

    void close();

    void close(boolean removeParent);

    ClientFactory getClientFactory();
}
