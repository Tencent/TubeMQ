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

package com.tencent.tubemq.server.common.aaaserver;

import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.broker.TubeBroker;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleCertificateBrokerHandler implements CertificateBrokerHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(SimpleCertificateBrokerHandler.class);
    private static final int MAX_VISIT_TOKEN_SIZE = 6; // at least 3 items

    private final TubeBroker tubeBroker;
    private final AtomicReference<List<Long>> visitTokenList =
            new AtomicReference<List<Long>>();
    private boolean enableProduceAuthenticate = false;
    private boolean enableProduceAuthorize = false;
    private boolean enableConsumeAuthenticate = false;
    private boolean enableConsumeAuthorize = false;

    public SimpleCertificateBrokerHandler(final TubeBroker tubeBroker) {
        this.tubeBroker = tubeBroker;
        this.visitTokenList.set(new ArrayList<Long>());
    }

    @Override
    public void configure(boolean enableProduceAuthenticate, boolean enableProduceAuthorize,
                          boolean enableConsumeAuthenticate, boolean enableConsumeAuthorize) {
        this.enableProduceAuthenticate = enableProduceAuthenticate;
        this.enableProduceAuthorize = enableProduceAuthorize;
        this.enableConsumeAuthenticate = enableConsumeAuthenticate;
        this.enableConsumeAuthorize = enableConsumeAuthorize;

    }

    @Override
    public void appendVisitToken(ClientMaster.MasterAuthorizedInfo authorizedInfo) {
        if (authorizedInfo == null) {
            return;
        }
        long curVisitToken = authorizedInfo.getVisitAuthorizedToken();
        List<Long> currList = visitTokenList.get();
        if (!currList.contains(curVisitToken)) {
            while (true) {
                currList = visitTokenList.get();
                if (currList.contains(curVisitToken)) {
                    return;
                }
                List<Long> updateList = new ArrayList<Long>(currList);
                while (updateList.size() >= MAX_VISIT_TOKEN_SIZE) {
                    updateList.remove(0);
                }
                updateList.add(curVisitToken);
                if (visitTokenList.compareAndSet(currList, updateList)) {
                    return;
                }
            }
        }
    }

    @Override
    public CertifiedResult identityValidUserInfo(final ClientBroker.AuthorizedInfo authorizedInfo,
                                                 boolean isProduce) {
        CertifiedResult result = new CertifiedResult();
        if (authorizedInfo == null) {
            result.setFailureResult(TErrCodeConstants.CERTIFICATE_FAILURE,
                    "Authorized Info is required!");
            return result;
        }
        long curVisitToken = authorizedInfo.getVisitAuthorizedToken();
        List<Long> currList = visitTokenList.get();
        if (!currList.contains(curVisitToken)) {
            result.setFailureResult(TErrCodeConstants.CERTIFICATE_FAILURE,
                    "Visit Authorized Token is invalid!");
            return result;
        }
        if ((isProduce && !enableProduceAuthenticate)
                || (!isProduce && !enableConsumeAuthenticate)) {
            result.setSuccessResult("", "");
            return result;
        }
        if (TStringUtils.isBlank(authorizedInfo.getAuthAuthorizedToken())) {
            result.setFailureResult(TErrCodeConstants.CERTIFICATE_FAILURE,
                    "authAuthorizedToken is Blank!");
            return result;
        }
        // process authAuthorizedToken info from certificate center begin
        // process authAuthorizedToken info from certificate center end
        // set userName, reAuth info
        result.setSuccessResult("", "", false);
        return result;
    }

    @Override
    public CertifiedResult validConsumeAuthorizeInfo(final String userName, final String groupName,
                                                     final String topicName, final Set<String> msgTypeLst,
                                                     boolean isRegister, String clientIp) {
        CertifiedResult result = new CertifiedResult();
        if (!enableConsumeAuthorize) {
            result.setSuccessResult("", "");
            return result;
        }

        // process authorize from authorize center begin
        // process authorize from authorize center end
        result.setSuccessResult("", "");
        return result;
    }

    @Override
    public CertifiedResult validProduceAuthorizeInfo(final String userName, final String topicName,
                                                     final String msgType, String clientIp) {
        CertifiedResult result = new CertifiedResult();
        if (!enableProduceAuthorize) {
            result.setSuccessResult("", "");
            return result;
        }

        // process authorize from authorize center begin
        // process authorize from authorize center end
        result.setSuccessResult("", "");
        return result;
    }


}
