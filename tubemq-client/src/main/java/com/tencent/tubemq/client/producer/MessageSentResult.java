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

package com.tencent.tubemq.client.producer;

import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.cluster.Partition;


public class MessageSentResult {
    private final boolean success;
    private final int errCode;
    private final String errMsg;
    private final Message message;
    private long messageId = TBaseConstants.META_VALUE_UNDEFINED;
    private Partition partition = null;

    public MessageSentResult(boolean success, int errCode, String errMsg,
                             Message message, long messageId, Partition partition) {
        this.success = success;
        this.errCode = errCode;
        this.errMsg = errMsg;
        this.message = message;
        this.messageId = messageId;
        this.partition = partition;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public int getErrCode() {
        return errCode;
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public Message getMessage() {
        return message;
    }

    public long getMessageId() {
        return messageId;
    }

    public Partition getPartition() {
        return this.partition;
    }
}
