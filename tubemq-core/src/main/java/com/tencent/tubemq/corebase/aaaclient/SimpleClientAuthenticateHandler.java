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

package com.tencent.tubemq.corebase.aaaclient;

import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import java.security.SecureRandom;
import org.apache.commons.codec.binary.StringUtils;


public class SimpleClientAuthenticateHandler implements ClientAuthenticateHandler {

    public SimpleClientAuthenticateHandler() {

    }

    @Override
    public ClientMaster.AuthenticateInfo.Builder genMasterAuthenticateToken(
            final String userName, final String usrPassWord) {
        long timestamp = System.currentTimeMillis();

        int nonce =
                new SecureRandom(StringUtils.getBytesUtf8(String.valueOf(timestamp))).nextInt(Integer.MAX_VALUE);
        String signature = TStringUtils.getAuthSignature(userName, usrPassWord, timestamp, nonce);
        ClientMaster.AuthenticateInfo.Builder authBuilder =
                ClientMaster.AuthenticateInfo.newBuilder();
        authBuilder.setUserName(userName);
        authBuilder.setTimestamp(timestamp);
        authBuilder.setNonce(nonce);
        authBuilder.setOthParams("");
        authBuilder.setSignature(signature);
        return authBuilder;
    }

    @Override
    public String genBrokerAuthenticateToken(final String userName, final String usrPassWord) {
        long timestamp = System.currentTimeMillis();
        int nonce =
                new SecureRandom(StringUtils.getBytesUtf8(String.valueOf(timestamp))).nextInt(Integer.MAX_VALUE);
        String signature = TStringUtils.getAuthSignature(userName, usrPassWord, timestamp, nonce);
        return new StringBuilder(512).append(userName)
                .append(TokenConstants.BLANK).append(timestamp)
                .append(TokenConstants.BLANK).append(nonce)
                .append(TokenConstants.BLANK).append(signature).toString();
    }
}
