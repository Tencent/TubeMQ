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

package com.tencent.tubemq.corebase.cluster;

import com.tencent.tubemq.corebase.TBaseConstants;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


public class ConsumerInfo implements Comparable<ConsumerInfo>, Serializable {

    private static final long serialVersionUID = 3095734962491009711L;

    private final String consumerId;
    private final String group;
    private final Set<String> topicSet;
    private final Map<String, TreeSet<String>> topicConditions;
    private boolean requireBound = false;
    private String sessionKey = "";
    private long startTime = TBaseConstants.META_VALUE_UNDEFINED;
    private int sourceCount = TBaseConstants.META_VALUE_UNDEFINED;
    private boolean overTLS = false;
    private Map<String, Long> requiredPartition;


    public ConsumerInfo(String consumerId,
                        boolean overTLS,
                        String group,
                        Set<String> topicSet,
                        Map<String, TreeSet<String>> topicConditions,
                        boolean requireBound,
                        String sessionKey,
                        long startTime,
                        int sourceCount,
                        Map<String, Long> requiredPartition) {
        this.consumerId = consumerId;
        this.overTLS = overTLS;
        this.group = group;
        this.topicSet = topicSet;
        if (topicConditions == null) {
            this.topicConditions =
                    new HashMap<String, TreeSet<String>>();
        } else {
            this.topicConditions = topicConditions;
        }
        this.requireBound = requireBound;
        this.sessionKey = sessionKey;
        this.startTime = startTime;
        this.sourceCount = sourceCount;
        this.requiredPartition = requiredPartition;
    }

    @Override
    public String toString() {
        StringBuilder sBuilder = new StringBuilder(512);
        sBuilder.append(consumerId);
        sBuilder.append("@");
        sBuilder.append(group);
        sBuilder.append(":");
        int count = 0;
        for (String topicItem : topicSet) {
            if (count++ > 0) {
                sBuilder.append(",");
            }
            sBuilder.append(topicItem);
        }
        sBuilder.append("@overTLS=").append(overTLS);
        return sBuilder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ConsumerInfo)) {
            return false;
        }
        final ConsumerInfo info = (ConsumerInfo) obj;
        return (this.consumerId.equals(info.getConsumerId()));
    }

    @Override
    public int compareTo(ConsumerInfo o) {
        if (!this.consumerId.equals(o.consumerId)) {
            return this.consumerId.compareTo(o.consumerId);
        }
        if (!this.group.equals(o.group)) {
            return this.group.compareTo(o.group);
        }
        return 0;
    }

    public boolean isRequireBound() {
        return this.requireBound;
    }

    public String getSessionKey() {
        return this.sessionKey;
    }

    public void setSessionKey(String sessionKey) {
        this.sessionKey = sessionKey;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public int getSourceCount() {
        return this.sourceCount;
    }

    public Map<String, Long> getRequiredPartition() {
        return this.requiredPartition;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getGroup() {
        return group;
    }

    public Set<String> getTopicSet() {
        return topicSet;
    }

    public Map<String, TreeSet<String>> getTopicConditions() {
        return topicConditions;
    }

    public boolean isOverTLS() {
        return overTLS;
    }

    public ConsumerInfo clone() {
        return new ConsumerInfo(this.consumerId, this.overTLS, this.group, this.topicSet,
                this.topicConditions, this.requireBound, this.sessionKey,
                this.startTime, this.sourceCount, this.requiredPartition);
    }

}
