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

package com.tencent.tubemq.server.broker.stats;

import java.util.concurrent.atomic.AtomicLong;

/***
 * Statistic of message, contains message's count and message's size.
 */
public class CountItem {
    AtomicLong msgCount = new AtomicLong(0);
    AtomicLong msgSize = new AtomicLong(0);

    public CountItem(long msgCount, long msgSize) {
        this.msgCount.set(msgCount);
        this.msgSize.set(msgSize);
    }

    public long getMsgSize() {
        return msgSize.get();
    }

    public void setMsgSize(long msgSize) {
        this.msgSize.set(msgSize);
    }

    public long getMsgCount() {
        return msgCount.get();
    }

    public void setMsgCount(long msgCount) {
        this.msgCount.set(msgCount);
    }

    public void appendMsg(final long msgCount, final long msgSize) {
        this.msgCount.addAndGet(msgCount);
        this.msgSize.addAndGet(msgSize);
    }

}
