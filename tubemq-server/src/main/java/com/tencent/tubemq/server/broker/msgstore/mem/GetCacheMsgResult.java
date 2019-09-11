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

package com.tencent.tubemq.server.broker.msgstore.mem;

import java.nio.ByteBuffer;
import java.util.List;

/***
 * Result set of read operation from memory.
 */
public class GetCacheMsgResult {
    public boolean isSuccess = false;
    public int retCode = -1;
    public String errInfo;
    public long startOffset;
    public int dltOffset;
    public long lastRdDataOff = -2;
    public int totalMsgSize;
    public List<ByteBuffer> cacheMsgList;


    public GetCacheMsgResult(boolean isSuccess, int retCode, long readOffset, String errInfo) {
        this.isSuccess = isSuccess;
        this.retCode = retCode;
        this.errInfo = errInfo;
        this.startOffset = readOffset;
        this.dltOffset = 0;
    }

    public GetCacheMsgResult(boolean isSuccess, int retCode, String errInfo,
                             long readOffset, int dltOffset, long lastRdDataOff,
                             int totalMsgSize, List<ByteBuffer> cacheMsgList) {
        this.isSuccess = isSuccess;
        this.retCode = retCode;
        this.errInfo = errInfo;
        this.startOffset = readOffset;
        this.dltOffset = dltOffset;
        this.lastRdDataOff = lastRdDataOff;
        this.totalMsgSize = totalMsgSize;
        this.cacheMsgList = cacheMsgList;
    }

}
