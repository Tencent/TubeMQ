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

package com.tencent.tubemq.corerpc.protocol;

import java.util.HashMap;
import java.util.Map;


public class ProtocolFactory {

    private static final Map<Integer, Class<? extends Protocol>> protocols =
            new HashMap<Integer, Class<? extends Protocol>>();

    static {
        registerProtocol(RpcProtocol.RPC_PROTOCOL_TCP, RpcProtocol.class);
        registerProtocol(RpcProtocol.RPC_PROTOCOL_TLS, RpcProtocol.class);
    }

    public static void registerProtocol(int type, Class<? extends Protocol> customProtocol) {
        protocols.put(type, customProtocol);
    }

    public static Class<? extends Protocol> getProtocol(int type) {
        return protocols.get(type);
    }

    public static Protocol getProtocolInstance(int type) throws Exception {
        return protocols.get(type).newInstance();
    }

}
