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

import com.tencent.tubemq.corebase.TBaseConstants;
import java.io.Serializable;


public class RequestWrapper implements Serializable {

    private static final long serialVersionUID = -2469749661773014443L;
    private int flagId = TBaseConstants.META_VALUE_UNDEFINED;
    private int serviceType = TBaseConstants.META_VALUE_UNDEFINED;
    private int protocolVersion = TBaseConstants.META_VALUE_UNDEFINED;
    private int protocolType = TBaseConstants.META_VALUE_UNDEFINED;
    private int serialNo;
    private long timeout;
    private int methodId = TBaseConstants.META_VALUE_UNDEFINED;
    private Object requestData;


    public RequestWrapper(int serviceType, int protocolVersion,
                          int flagId, long timeout) {
        this.serviceType = serviceType;
        this.protocolVersion = protocolVersion;
        this.flagId = flagId;
        this.timeout = timeout;
    }

    public RequestWrapper(int serviceType,
                          int protocolType, int protocolVersion,
                          int flagId, long timeout) {
        this.serviceType = serviceType;
        this.protocolType = protocolType;
        this.protocolVersion = protocolVersion;
        this.flagId = flagId;
        this.timeout = timeout;
    }


    public int getFlagId() {
        return flagId;
    }

    public void setFlagId(int flagId) {
        this.flagId = flagId;
    }

    public int getProtocolType() {
        return protocolType;
    }

    public void setProtocolType(int protocolType) {
        this.protocolType = protocolType;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public int getServiceType() {
        return serviceType;
    }

    public void setServiceType(int serviceType) {
        this.serviceType = serviceType;
    }

    public int getSerialNo() {
        return serialNo;
    }

    public void setSerialNo(int serialNo) {
        this.serialNo = serialNo;
    }

    public Object getRequestData() {
        return requestData;
    }

    public void setRequestData(Object requestData) {
        this.requestData = requestData;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public int getMethodId() {
        return methodId;
    }

    public void setMethodId(int methodId) {
        this.methodId = methodId;
    }

}
