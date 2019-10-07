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

package com.tencent.tubemq.server.broker.metadata;

import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.common.TStatusConstants;

/***
 * Topic's metadata. Contains topic name, partitions count, etc.
 */
public class TopicMetadata {
    // topic name.
    private String topic;
    // metadata status.
    private int statusId = TStatusConstants.STATUS_TOPIC_OK;
    // topic store count.
    private int numTopicStores = 1;
    // topic partition count.
    private int numPartitions = 1;
    // data will be flushed to disk when elapse unflushInterval milliseconds since last flush operation.
    private int unflushThreshold = 1000;
    // data will be flushed to disk when unflushed message count exceed this.
    private int unflushInterval = 10000;
    @Deprecated
    private int unflushDataHold = 10000;
    // enable produce data to topic.
    private boolean acceptPublish = true;
    // enable consume data from topic.
    private boolean acceptSubscribe = true;
    // path to store topic's data in disk.
    private String dataPath;
    @Deprecated
    private String deleteWhen = "0 0 6,18 * * ?";
    // expire policy.
    private String deletePolicy = "delete,168h";
    // the max cache size for topic.
    private int memCacheMsgSize = 1 * 1024 * 1024;
    // the max cache message count for topic.
    private int memCacheMsgCnt = 5 * 1024;
    // the max interval(milliseconds) that topic's memory cache will flush to disk.
    private int memCacheFlushIntvl = 20000;

    /***
     * Build TopicMetadata from brokerDefMetadata(default config) and topicMetaConfInfo(custom config).
     *
     * @param brokerDefMetadata
     * @param topicMetaConfInfo
     */
    public TopicMetadata(final BrokerDefMetadata brokerDefMetadata, String topicMetaConfInfo) {
        if (TStringUtils.isBlank(topicMetaConfInfo)) {
            return;
        }
        String[] topicConfInfoArr =
                topicMetaConfInfo.split(TokenConstants.ATTR_SEP);
        this.topic = topicConfInfoArr[0];
        if (TStringUtils.isBlank(topicConfInfoArr[1])) {
            this.numPartitions = brokerDefMetadata.getNumPartitions();
        } else {
            this.numPartitions = Integer.parseInt(topicConfInfoArr[1]);
        }
        if (TStringUtils.isBlank(topicConfInfoArr[2])) {
            this.acceptPublish = brokerDefMetadata.isAcceptPublish();
        } else {
            this.acceptPublish = Boolean.parseBoolean(topicConfInfoArr[2]);
        }
        if (TStringUtils.isBlank(topicConfInfoArr[3])) {
            this.acceptSubscribe = brokerDefMetadata.isAcceptSubscribe();
        } else {
            this.acceptSubscribe = Boolean.parseBoolean(topicConfInfoArr[3]);
        }
        if (TStringUtils.isBlank(topicConfInfoArr[4])) {
            this.unflushThreshold = brokerDefMetadata.getUnflushThreshold();
        } else {
            this.unflushThreshold = Integer.parseInt(topicConfInfoArr[4]);
        }
        if (TStringUtils.isBlank(topicConfInfoArr[5])) {
            this.unflushInterval = brokerDefMetadata.getUnflushInterval();
        } else {
            this.unflushInterval = Integer.parseInt(topicConfInfoArr[5]);
        }
        if (TStringUtils.isBlank(topicConfInfoArr[6])) {
            this.deleteWhen = brokerDefMetadata.getDeleteWhen();
        } else {
            this.deleteWhen = topicConfInfoArr[6];
        }
        if (TStringUtils.isBlank(topicConfInfoArr[7])) {
            this.deletePolicy = brokerDefMetadata.getDeletePolicy();
        } else {
            this.deletePolicy = topicConfInfoArr[7];
        }
        if (TStringUtils.isBlank(topicConfInfoArr[8])) {
            this.numTopicStores = brokerDefMetadata.getNumTopicStores();
        } else {
            this.numTopicStores = Integer.parseInt(topicConfInfoArr[8]);
        }
        if (TStringUtils.isBlank(topicConfInfoArr[9])) {
            this.statusId = TStatusConstants.STATUS_TOPIC_OK;
        } else {
            this.statusId = Integer.parseInt(topicConfInfoArr[9]);
        }
        if (TStringUtils.isBlank(topicConfInfoArr[10])) {
            this.unflushDataHold = brokerDefMetadata.getUnflushDataHold();
        } else {
            this.unflushDataHold = Integer.parseInt(topicConfInfoArr[10]);
        }
        if (TStringUtils.isBlank(topicConfInfoArr[11])) {
            this.memCacheMsgSize = brokerDefMetadata.getMemCacheMsgSize();
        } else {
            this.memCacheMsgSize = Integer.parseInt(topicConfInfoArr[11]) * 1024 * 512;
        }
        if (TStringUtils.isBlank(topicConfInfoArr[12])) {
            this.memCacheMsgCnt = brokerDefMetadata.getMemCacheMsgCnt();
        } else {
            this.memCacheMsgCnt = Integer.parseInt(topicConfInfoArr[12]) * 512;
        }
        if (TStringUtils.isBlank(topicConfInfoArr[13])) {
            this.memCacheFlushIntvl = brokerDefMetadata.getMemCacheFlushInterval();
        } else {
            this.memCacheFlushIntvl = Integer.parseInt(topicConfInfoArr[13]);
        }
    }

    private TopicMetadata(String topic, int unflushThreshold,
                          int unflushInterval, String dataPath,
                          String deleteWhen, String deletePolicy,
                          int numPartitions, boolean acceptPublish,
                          boolean acceptSubscribe, int statusId,
                          int numTopicStores, int memCacheMsgSize,
                          int memCacheMsgCnt, int memCacheFlushIntvl) {
        this.topic = topic;
        this.unflushThreshold = unflushThreshold;
        this.unflushInterval = unflushInterval;
        this.dataPath = dataPath;
        this.deleteWhen = deleteWhen;
        this.deletePolicy = deletePolicy;
        this.numPartitions = numPartitions;
        this.acceptPublish = acceptPublish;
        this.acceptSubscribe = acceptSubscribe;
        this.statusId = statusId;
        this.numTopicStores = numTopicStores;
        this.memCacheMsgSize = memCacheMsgSize;
        this.memCacheMsgCnt = memCacheMsgCnt;
        this.memCacheFlushIntvl = memCacheFlushIntvl;
    }

    @Override
    public TopicMetadata clone() {
        return new TopicMetadata(this.topic, this.unflushThreshold,
                this.unflushInterval, this.dataPath,
                this.deleteWhen, this.deletePolicy,
                this.numPartitions, this.acceptPublish,
                this.acceptSubscribe, this.statusId,
                this.numTopicStores, this.memCacheMsgSize,
                this.memCacheMsgCnt, this.memCacheFlushIntvl);
    }

    public boolean isAcceptPublish() {
        return this.acceptPublish;
    }

    public boolean isAcceptSubscribe() {
        return this.acceptSubscribe;
    }

    public int getNumTopicStores() {
        return numTopicStores;
    }

    public int getNumPartitions() {
        return this.numPartitions;
    }

    public String getDeletePolicy() {
        return this.deletePolicy;
    }

    public void setDeletePolicy(final String deletePolicy) {
        this.deletePolicy = deletePolicy;
    }

    public String getDeleteWhen() {
        return this.deleteWhen;
    }

    public String getDataPath() {
        return this.dataPath;
    }

    public String getTopic() {
        return this.topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public int getUnflushThreshold() {
        return this.unflushThreshold;
    }

    public void setUnflushThreshold(final int unflushThreshold) {
        this.unflushThreshold = unflushThreshold;
    }

    public int getUnflushInterval() {
        return this.unflushInterval;
    }

    public void setUnflushInterval(final int unflushInterval) {
        this.unflushInterval = unflushInterval;
    }

    public int getStatusId() {
        return statusId;
    }

    public void setStatusId(int statusId) {
        this.statusId = statusId;
    }

    public boolean isValidTopic() {
        return this.statusId == TStatusConstants.STATUS_TOPIC_OK;
    }

    public int getMemCacheMsgSize() {
        return memCacheMsgSize;
    }

    public int getMemCacheMsgCnt() {
        return memCacheMsgCnt;
    }

    public int getMemCacheFlushIntvl() {
        return memCacheFlushIntvl;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.acceptPublish ? 1231 : 1237);
        result = prime * result + (this.acceptSubscribe ? 1231 : 1237);
        result = prime * result + (this.dataPath == null ? 0 : this.dataPath.hashCode());
        result = prime * result + (this.deletePolicy == null ? 0 : this.deletePolicy.hashCode());
        result = prime * result + (this.deleteWhen == null ? 0 : this.deleteWhen.hashCode());
        result = prime * result + this.numPartitions;
        result = prime * result + this.numTopicStores;
        result = prime * result + (this.topic == null ? 0 : this.topic.hashCode());
        result = prime * result + this.unflushInterval;
        result = prime * result + this.unflushThreshold;
        result = prime * result + this.statusId;
        result = prime * result + this.memCacheMsgSize;
        result = prime * result + this.memCacheMsgCnt;
        result = prime * result + this.memCacheFlushIntvl;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        // #lizard forgives
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        TopicMetadata other = (TopicMetadata) obj;
        if (this.acceptPublish != other.acceptPublish) {
            return false;
        }
        if (this.acceptSubscribe != other.acceptSubscribe) {
            return false;
        }
        if (this.dataPath == null) {
            if (other.dataPath != null) {
                return false;
            }
        } else if (!this.dataPath.equals(other.dataPath)) {
            return false;
        }
        if (this.deletePolicy == null) {
            if (other.deletePolicy != null) {
                return false;
            }
        } else if (!this.deletePolicy.equals(other.deletePolicy)) {
            return false;
        }
        if (this.deleteWhen == null) {
            if (other.deleteWhen != null) {
                return false;
            }
        } else if (!this.deleteWhen.equals(other.deleteWhen)) {
            return false;
        }
        if (this.numPartitions != other.numPartitions) {
            return false;
        }
        if (this.topic == null) {
            if (other.topic != null) {
                return false;
            }
        } else if (!this.topic.equals(other.topic)) {
            return false;
        }
        if (this.unflushInterval != other.unflushInterval) {
            return false;
        }
        if (this.unflushThreshold != other.unflushThreshold) {
            return false;
        }
        if (this.statusId != other.statusId) {
            return false;
        }
        if (this.numTopicStores != other.numTopicStores) {
            return false;
        }
        if (this.memCacheMsgSize != other.memCacheMsgSize) {
            return false;
        }
        if (this.memCacheMsgCnt != other.memCacheMsgCnt) {
            return false;
        }
        if (this.memCacheFlushIntvl != other.memCacheFlushIntvl) {
            return false;
        }

        return true;
    }

    /***
     * Each property will be compared, in case of the new added properties.
     *
     * @param other
     * @return
     */
    public boolean isPropertyEquals(final TopicMetadata other) {
        return (this.numPartitions == other.numPartitions
                && this.unflushInterval == other.unflushInterval
                && this.unflushThreshold == other.unflushThreshold
                && this.memCacheMsgSize == other.memCacheMsgSize
                && this.memCacheMsgCnt == other.memCacheMsgCnt
                && this.memCacheFlushIntvl == other.memCacheFlushIntvl);
    }

    @Override
    public String toString() {
        return new StringBuilder(512).append("TopicMetadata [topic=").append(this.topic)
                .append(", unflushThreshold=").append(this.unflushThreshold)
                .append(", unflushInterval=").append(this.unflushInterval)
                .append(", dataPath=").append(this.dataPath)
                .append(", deleteWhen=").append(this.deleteWhen)
                .append(", deletePolicy=").append(this.deletePolicy)
                .append(", numPartitions=").append(this.numPartitions)
                .append(", acceptPublish=").append(this.acceptPublish)
                .append(", acceptSubscribe=").append(this.acceptSubscribe)
                .append(", statusId=").append(this.statusId)
                .append(", numTopicStores=").append(this.numTopicStores)
                .append(", memCacheMsgSizeInMs=").append(this.memCacheMsgSize / 1024 / 512)
                .append(", memCacheMsgCntInK=").append(this.memCacheMsgCnt / 512)
                .append(", memCacheFlushIntvl=").append(this.memCacheFlushIntvl)
                .append("]").toString();
    }
}
