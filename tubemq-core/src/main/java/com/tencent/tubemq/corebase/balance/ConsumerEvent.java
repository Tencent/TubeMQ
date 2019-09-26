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

package com.tencent.tubemq.corebase.balance;

import com.tencent.tubemq.corebase.cluster.SubscribeInfo;
import java.util.ArrayList;
import java.util.List;


public class ConsumerEvent {

    private long rebalanceId;
    private EventType type;
    private EventStatus status;
    private List<SubscribeInfo> subscribeInfoList =
            new ArrayList<SubscribeInfo>();


    public ConsumerEvent(long rebalanceId,
                         EventType type,
                         List<SubscribeInfo> subscribeInfoList,
                         EventStatus status) {
        this.rebalanceId = rebalanceId;
        this.type = type;
        if (subscribeInfoList != null) {
            this.subscribeInfoList = subscribeInfoList;
        }
        this.status = status;
    }

    public long getRebalanceId() {
        return rebalanceId;
    }

    public void setRebalanceId(long rebalanceId) {
        this.rebalanceId = rebalanceId;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public EventStatus getStatus() {
        return status;
    }

    public void setStatus(EventStatus status) {
        this.status = status;
    }

    public List<SubscribeInfo> getSubscribeInfoList() {
        return subscribeInfoList;
    }

    public void setSubscribeInfoList(List<SubscribeInfo> subscribeInfoList) {
        if (subscribeInfoList != null) {
            this.subscribeInfoList = subscribeInfoList;
        }
    }

    public StringBuilder toStrBuilder(final StringBuilder sBuilder) {
        return sBuilder.append("ConsumerEvent [rebalanceId=").append(rebalanceId)
                .append(", type=").append(type).append(", status=").append(status)
                .append(", subscribeInfoList=").append(subscribeInfoList).append("]");
    }
}
