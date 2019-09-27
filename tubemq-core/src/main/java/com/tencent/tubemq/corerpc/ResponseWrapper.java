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

import com.tencent.tubemq.corerpc.exception.StandbyException;
import java.io.Serializable;


public class ResponseWrapper implements Serializable {

    private static final long serialVersionUID = -3852197088007144687L;

    private int serialNo;
    private int flagId = 0;
    private int serviceType;
    private int protocolVersion;
    private boolean success = false;
    private int methodId;
    private Object responseData;
    private String errMsg;
    private String stackTrace;


    public ResponseWrapper(int flagId, int serialNo,
                           int serviceType, int serverVersion,
                           int methodId, Object responseData) {
        this.serialNo = serialNo;
        this.serviceType = serviceType;
        this.protocolVersion = serverVersion;
        this.flagId = flagId;
        this.methodId = methodId;
        this.responseData = responseData;
        this.success = true;
    }

    public ResponseWrapper(int flagId, int serialNo,
                           int serviceType, int serverVersion,
                           Throwable exception) {
        this.serialNo = serialNo;
        this.flagId = flagId;
        this.serviceType = serviceType;
        this.protocolVersion = serverVersion;
        String errorClass = null;
        String error = null;
        if (exception.getCause() != null
                && exception.getCause() instanceof StandbyException) {
            errorClass = exception.getCause().getClass().getName();
            error = exception.getCause().getMessage();
        } else {
            errorClass = exception.getClass().getName();
            error = exception.getMessage();
        }
        this.errMsg = errorClass;
        this.stackTrace = error;
        if (this.errMsg == null) {
            this.errMsg = "";
        }
        if (this.stackTrace == null) {
            this.stackTrace = "";
        }
    }

    public ResponseWrapper(int flagId, int serialNo,
                           int serviceType, int serverVersion,
                           String errorMsg, String stackTrace) {
        this.serialNo = serialNo;
        this.flagId = flagId;
        this.serviceType = serviceType;
        this.protocolVersion = serverVersion;
        this.errMsg = errorMsg;
        this.stackTrace = stackTrace;
        if (this.errMsg == null) {
            this.errMsg = "";
        }
        if (this.stackTrace == null) {
            this.stackTrace = "";
        }
    }

    public int getFlagId() {
        return flagId;
    }

    public void setFlagId(int flagId) {
        this.flagId = flagId;
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

    public int getMethodId() {
        return methodId;
    }

    public void setMethodId(int methodId) {
        this.methodId = methodId;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public void getClientProtocolVersion(int clientVersion) {
        this.protocolVersion = clientVersion;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Object getResponseData() {
        return responseData;
    }

    public void setResponseData(Object responseData) {
        this.responseData = responseData;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }


}
