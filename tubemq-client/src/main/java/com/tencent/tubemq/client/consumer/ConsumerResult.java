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

package com.tencent.tubemq.client.consumer;

import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.TBaseConstants;
import java.util.ArrayList;
import java.util.List;


public class ConsumerResult {
    private boolean success = false;
    private int errCode = TBaseConstants.META_VALUE_UNDEFINED;
    private String errMsg = "";
    private String topicName = "";
    private String partitionKey = "";
    private long currOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private String confirmContext = "";
    private List<Message> messageList = new ArrayList<Message>();


    public ConsumerResult(int errCode, String errMsg) {
        this.success = false;
        this.errCode = errCode;
        this.errMsg = errMsg;
    }

    public ConsumerResult(FetchContext taskContext) {
        this.success = taskContext.isSuccess();
        this.errCode = taskContext.getErrCode();
        this.errMsg = taskContext.getErrMsg();
        this.topicName = taskContext.getPartition().getTopic();
        this.partitionKey = taskContext.getPartition().getPartitionKey();
        if (this.success) {
            this.currOffset = taskContext.getCurrOffset();
            this.messageList = taskContext.getMessageList();
            this.confirmContext = taskContext.getConfirmContext();
        }
    }

    public ConsumerResult(boolean success, String errMsg, String topicName, String partitionKey) {
        this.success = success;
        this.errMsg = errMsg;
        this.partitionKey = partitionKey;
        this.topicName = topicName;
    }

    public void setConfirmProcessResult(boolean success, String errMsg, long currOffset) {
        this.success = success;
        this.errMsg = errMsg;
        if (currOffset >= 0) {
            this.currOffset = currOffset;
        }
    }

    public boolean isSuccess() {
        return success;
    }

    public int getErrCode() {
        return errCode;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getPartitionKey() {
        return this.partitionKey;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public final String getConfirmContext() {
        return confirmContext;
    }

    public long getCurrOffset() {
        return currOffset;
    }

    public final List<Message> getMessageList() {
        return messageList;
    }
}
