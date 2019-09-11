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

package com.tencent.tubemq.server.common.paramcheck;

import com.tencent.tubemq.corebase.TErrCodeConstants;

public class ParamCheckResult {
    public boolean result;
    public int errCode;
    public String errMsg;
    public Object checkData;

    public ParamCheckResult() {
        this.result = false;
        this.errCode = TErrCodeConstants.BAD_REQUEST;
        this.errMsg = "Unset object!";
    }

    public ParamCheckResult(boolean result, int errCode, final String errMsg) {
        this.result = result;
        this.errCode = errCode;
        this.errMsg = errMsg;
    }

    public ParamCheckResult(Object checkData) {
        this.result = true;
        this.errCode = TErrCodeConstants.SUCCESS;
        this.errMsg = "Ok";
        this.checkData = checkData;
    }

    public void setCheckResult(boolean result, int errCode, final String errMsg) {
        this.result = result;
        this.errCode = errCode;
        this.errMsg = errMsg;
    }

    public void setCheckData(Object checkData) {
        this.result = true;
        this.errCode = TErrCodeConstants.SUCCESS;
        this.errMsg = "Ok";
        this.checkData = checkData;
    }
}
