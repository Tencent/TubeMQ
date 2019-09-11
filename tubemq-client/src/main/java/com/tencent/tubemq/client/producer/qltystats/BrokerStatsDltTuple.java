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

package com.tencent.tubemq.client.producer.qltystats;

/**
 * Broker quality statistics.
 * <p>
 * Record the request number and success response number of a broker. We analyze the broker quality
 * based on these information.
 */
public class BrokerStatsDltTuple {

    private long sendNum = 0L;
    private long succRecvNum = 0L;

    public BrokerStatsDltTuple(long succRecvNum, long sendNum) {
        this.succRecvNum = succRecvNum;
        this.sendNum = sendNum;
    }

    public long getSendNum() {
        return sendNum;
    }

    public void setSendNum(long sendNum) {
        this.sendNum = sendNum;
    }

    public long getSuccRecvNum() {
        return succRecvNum;
    }

    public void setSuccRecvNum(long succRecvNum) {
        this.succRecvNum = succRecvNum;
    }

    @Override
    public String toString() {
        return "{sendNum=" + this.sendNum + ",succRecvNum=" + this.succRecvNum + "}";
    }
}
