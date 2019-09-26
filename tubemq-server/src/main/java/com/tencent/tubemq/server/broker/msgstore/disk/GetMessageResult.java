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

package com.tencent.tubemq.server.broker.msgstore.disk;

import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.TransferedMessage;
import com.tencent.tubemq.server.broker.stats.CountItem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/***
 * Broker's reply to Consumer's GetMessage request.
 */
public class GetMessageResult {
    public boolean isSuccess;
    public int retCode = -1;
    public String errInfo;
    public long reqOffset;
    public int lastReadOffset = -2;
    public long lastRdDataOffset;
    public int totalMsgSize;
    public long waitTime = -1;
    public boolean isSlowFreq = false;
    public boolean isFromSsdFile = false;
    public HashMap<String, CountItem> tmpCounters = new HashMap<String, CountItem>();
    public List<TransferedMessage> transferedMessageList = new ArrayList<TransferedMessage>();


    public GetMessageResult(boolean isSuccess, int retCode, final String errInfo,
                            final long reqOffset, final int lastReadOffset,
                            final long lastRdDataOffset, final int totalSize,
                            HashMap<String, CountItem> tmpCounters,
                            List<TransferedMessage> transferedMessageList) {
        this(isSuccess, retCode, errInfo, reqOffset, lastReadOffset,
                lastRdDataOffset, totalSize, tmpCounters, transferedMessageList, false);
    }


    public GetMessageResult(boolean isSuccess, int retCode, final String errInfo,
                            final long reqOffset, final int lastReadOffset,
                            final long lastRdDataOffset, final int totalSize,
                            HashMap<String, CountItem> tmpCounters,
                            List<TransferedMessage> transferedMessageList,
                            boolean isFromSsdFile) {
        this.isSuccess = isSuccess;
        this.errInfo = errInfo;
        this.retCode = retCode;
        this.tmpCounters = tmpCounters;
        this.reqOffset = reqOffset;
        this.lastReadOffset = lastReadOffset;
        this.lastRdDataOffset = lastRdDataOffset;
        this.totalMsgSize = totalSize;
        this.transferedMessageList = transferedMessageList;
        this.isFromSsdFile = isFromSsdFile;
    }

    public GetMessageResult(boolean isSuccess,
                            int retCode,
                            final long reqOffset,
                            final int lastReadOffset,
                            final String errInfo) {
        this.isSuccess = isSuccess;
        this.retCode = retCode;
        this.errInfo = errInfo;
        this.reqOffset = reqOffset;
        this.lastReadOffset = lastReadOffset;
    }

    public GetMessageResult(boolean isSuccess, int retCode,
                            final long reqOffset, final int lastReadOffset,
                            final long waitTime, final String errInfo) {
        this.isSuccess = isSuccess;
        this.retCode = retCode;
        this.errInfo = errInfo;
        this.reqOffset = reqOffset;
        this.lastReadOffset = lastReadOffset;
        this.waitTime = waitTime;
    }

    public boolean isSlowFreq() {
        return isSlowFreq;
    }

    public void setSlowFreq(boolean slowFreq) {
        isSlowFreq = slowFreq;
    }

    public long getWaitTime() {
        return waitTime;
    }

    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    public HashMap<String, CountItem> getTmpCounters() {
        return tmpCounters;
    }

    public void setTmpCounters(HashMap<String, CountItem> tmpCounters) {
        this.tmpCounters = tmpCounters;
    }

    public List<TransferedMessage> getTransferedMessageList() {
        return transferedMessageList;
    }

    public void setTransferedMessageList(List<TransferedMessage> transferedMessageList) {
        this.transferedMessageList = transferedMessageList;
    }

    public boolean isFromSsdFile() {
        return isFromSsdFile;
    }

    public void setFromSsdFile(boolean isFromSsdFile) {
        this.isFromSsdFile = isFromSsdFile;
    }

    public int getRetCode() {
        return retCode;
    }

    public void setRetCode(int retCode) {
        this.retCode = retCode;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }

    public String getErrInfo() {
        return errInfo;
    }

    public void setErrInfo(String errInfo) {
        this.errInfo = errInfo;
    }

    public int getLastReadOffset() {
        return lastReadOffset;
    }

    public void setLastReadOffset(int lastReadOffset) {
        this.lastReadOffset = lastReadOffset;
    }

    public long getReqOffset() {
        return reqOffset;
    }

    public void setReqOffset(long reqOffset) {
        this.reqOffset = reqOffset;
    }
}
