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

package com.tencent.tubemq.corerpc.utils;

import javax.net.ssl.SSLEngine;
import org.junit.Assert;

/***
 * TSSLEngineUtil test.
 */
public class TSSLEngineUtilTest {

    @org.junit.Test
    public void createSSLEngine() {
        // key store file path
        String keyStorePath = "./src/test/resource/tubeServer.keystore";
        // key store file password
        String keyStorePassword = "tubeserver";
        // trust store file path
        String trustStorePath = "./src/test/resource/tubeServerTrustStore.keystore";
        // trust store file password
        String trustStorePassword = "tubeserver";
        SSLEngine sslEngine = null;
        try {
            // create engine
            sslEngine = TSSLEngineUtil.createSSLEngine(keyStorePath, trustStorePath,
                    keyStorePassword, trustStorePassword, true, false);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        boolean needClientAuth = sslEngine.getNeedClientAuth();
        Assert.assertTrue(!needClientAuth);
    }
}
