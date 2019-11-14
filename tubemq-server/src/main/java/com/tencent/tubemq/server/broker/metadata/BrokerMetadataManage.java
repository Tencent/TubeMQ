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

import com.tencent.tubemq.corebase.policies.FlowCtrlRuleHandler;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.common.TStatusConstants;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Broker's metadata management util. Metadata contains broker's default configurations, topics,
 * topic that will be deleted, and broker's policy definition.
 * Metadata is got from Master service, it will refresh in heartbeat between broker and master.
 */
public class BrokerMetadataManage implements MetadataManage {
    private static final Logger logger = LoggerFactory.getLogger(BrokerMetadataManage.class);

    protected final PropertyChangeSupport propertyChangeSupport =
            new PropertyChangeSupport(this);
    // the rule handler of flow control.
    private final FlowCtrlRuleHandler flowCtrlRuleHandler =
            new FlowCtrlRuleHandler(true);
    // broker's config check sum.
    private int brokerConfCheckSumId = 0;
    // broker's metadata Id.
    private long brokerMetadataConfId = 0;
    // broker's metadata in String format.
    private String brokerDefMetaConfInfo = "";
    // broker's topic's config list.
    private List<String> topicMetaConfInfoLst = new ArrayList<String>();
    // topic in this broker.
    private List<String> topics = new ArrayList<String>();
    // broker's default metadata.
    private BrokerDefMetadata brokerDefMetadata = new BrokerDefMetadata();
    // topic with custom config.
    private ConcurrentHashMap<String/* topic */, TopicMetadata> topicConfigMap =
            new ConcurrentHashMap<String, TopicMetadata>();
    // topics will be closed.
    private Map<String/* topic */, Integer> closedTopicMap =
            new ConcurrentHashMap<String, Integer>();
    // topics will be removed.
    private Map<String/* topic */, TopicMetadata> removedTopicConfigMap =
            new ConcurrentHashMap<String, TopicMetadata>();
    private long lastRptBrokerMetaConfId = 0;

    public BrokerMetadataManage() {

    }

    @Override
    public void close(long waitTimeMs) {

    }

    @Override
    public int getBrokerConfCheckSumId() {
        return brokerConfCheckSumId;
    }

    @Override
    public long getBrokerMetadataConfId() {
        return brokerMetadataConfId;
    }

    @Override
    public String getBrokerDefMetaConfInfo() {
        return brokerDefMetaConfInfo;
    }

    @Override
    public List<String> getTopicMetaConfInfoLst() {
        return topicMetaConfInfoLst;
    }

    @Override
    public FlowCtrlRuleHandler getFlowCtrlRuleHandler() {
        return this.flowCtrlRuleHandler;
    }

    @Override
    public List<String> getTopics() {
        return topics;
    }

    @Override
    public String getDefDeletePolicy() {
        return brokerDefMetadata.getDeletePolicy();
    }

    @Override
    public String getTopicDeletePolicy(String topic) {
        TopicMetadata metadata = topicConfigMap.get(topic);
        if (metadata == null) {
            return brokerDefMetadata.getDeletePolicy();
        }
        return metadata.getDeletePolicy();
    }

    @Override
    public int getNumPartitions(final String topic) {
        final TopicMetadata topicMetadata = topicConfigMap.get(topic);
        return topicMetadata != null
                ? topicMetadata.getNumPartitions() : brokerDefMetadata.getNumPartitions();
    }

    @Override
    public int getNumTopicStores(final String topic) {
        final TopicMetadata topicMetadata = topicConfigMap.get(topic);
        return topicMetadata != null
                ? topicMetadata.getNumTopicStores() : brokerDefMetadata.getNumTopicStores();
    }

    @Override
    public BrokerDefMetadata getBrokerDefMetadata() {
        return brokerDefMetadata;
    }

    @Override
    public TopicMetadata getTopicMetadata(final String topic) {
        return topicConfigMap.get(topic);
    }

    public ConcurrentHashMap<String, TopicMetadata> getTopicConfigMap() {
        return topicConfigMap;
    }

    @Override
    public boolean isClosedTopic(final String topic) {
        return this.closedTopicMap.get(topic) != null;
    }

    @Override
    public Integer getCosedTopicStatusId(final String topic) {
        return this.closedTopicMap.get(topic);
    }

    public Map<String, Integer> getClosedTopicMap() {
        return closedTopicMap;
    }

    @Override
    public Map<String, TopicMetadata> getRemovedTopicConfigMap() {
        return removedTopicConfigMap;
    }

    /***
     * Get hard removed topics. Hard removed means the disk files is deleted, cannot be recovery.
     * Topic will be deleted in two phases, the first is mark topic's file delete, the second is delete the disk files.
     *
     * @return
     */
    @Override
    public List<String> getHardRemovedTopics() {
        List<String> targetTopics = new ArrayList<String>();
        for (Map.Entry<String, TopicMetadata> entry
                : this.removedTopicConfigMap.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (entry.getValue().getStatusId() == TStatusConstants.STATUS_TOPIC_HARD_REMOVE) {
                targetTopics.add(entry.getKey());
            }
        }
        return targetTopics;
    }

    @Override
    public boolean isBrokerMetadataChanged() {
        return this.brokerMetadataConfId != this.lastRptBrokerMetaConfId;
    }

    @Override
    public long getLastRptBrokerMetaConfId() {
        return lastRptBrokerMetaConfId;
    }

    @Override
    public void setLastRptBrokerMetaConfId(long lastRptBrokerMetaConfId) {
        this.lastRptBrokerMetaConfId = lastRptBrokerMetaConfId;
    }

    /***
     * Update broker's metadata in memory, then fire these metadata take effect.
     * These params are got from Master Service.
     *
     * @param newBrokerMetaConfId
     * @param newConfCheckSumId
     * @param newBrokerDefMetaConfInfo
     * @param newTopicMetaConfInfoLst
     * @param isForce
     * @param sb
     */
    @Override
    public void updateBrokerTopicConfigMap(long newBrokerMetaConfId,
                                           int newConfCheckSumId,
                                           String newBrokerDefMetaConfInfo,
                                           List<String> newTopicMetaConfInfoLst,
                                           boolean isForce,
                                           final StringBuilder sb) {
        if ((!isForce)
                && (this.brokerMetadataConfId == newBrokerMetaConfId)
                && (this.brokerConfCheckSumId == newConfCheckSumId)) {
            logger.info(sb
                    .append("[Metadata Manage] Broker topic configure is equal, not update! curBrokerConfId is ")
                    .append(this.brokerMetadataConfId).append("received newBrokerMetaConfId is ")
                    .append(newBrokerMetaConfId).toString());
            sb.delete(0, sb.length());
            return;
        }
        if (TStringUtils.isBlank(newBrokerDefMetaConfInfo)) {
            logger.error("[Metadata Manage] received broker default configure is Blank, not update");
            return;
        }
        this.brokerDefMetadata = new BrokerDefMetadata(newBrokerDefMetaConfInfo);
        this.brokerDefMetaConfInfo = newBrokerDefMetaConfInfo;
        if ((newTopicMetaConfInfoLst == null)
                || newTopicMetaConfInfoLst.isEmpty()) {
            this.brokerMetadataConfId = newBrokerMetaConfId;
            this.brokerConfCheckSumId = newConfCheckSumId;
            logger.error("[Metadata Manage] received broker topic info is Blank, not update");
            return;
        }
        List<String> newTopics = new ArrayList<String>();
        Map<String/* topic */, Integer> tmpInvalidTopicMap =
                new ConcurrentHashMap<String, Integer>();
        ConcurrentHashMap<String/* topic */, TopicMetadata> newTopicConfigMap =
                new ConcurrentHashMap<String, TopicMetadata>();
        for (String strTopicConfInfo : newTopicMetaConfInfoLst) {
            if (TStringUtils.isBlank(strTopicConfInfo)) {
                continue;
            }
            TopicMetadata topicMetadata = new TopicMetadata(brokerDefMetadata, strTopicConfInfo);
            if (!topicMetadata.isValidTopic()) {
                tmpInvalidTopicMap.put(topicMetadata.getTopic(),
                        topicMetadata.getStatusId());
            }
            newTopics.add(topicMetadata.getTopic());
            newTopicConfigMap.put(topicMetadata.getTopic(), topicMetadata);
        }
        this.topicMetaConfInfoLst = newTopicMetaConfInfoLst;
        this.closedTopicMap = tmpInvalidTopicMap;
        this.brokerMetadataConfId = newBrokerMetaConfId;
        this.brokerConfCheckSumId = newConfCheckSumId;
        Collections.sort(newTopics);
        if (!newTopicConfigMap.equals(this.topicConfigMap)) {
            Map<String, TopicMetadata> oldTopicConfigMap = this.topicConfigMap;
            this.topics = newTopics;
            this.topicConfigMap = newTopicConfigMap;
            this.propertyChangeSupport
                    .firePropertyChange("topicConfigMap", oldTopicConfigMap, newTopicConfigMap);
        }
        this.propertyChangeSupport.firePropertyChange("unflushInterval", null, null);
    }

    /***
     * Update will be deleted topics info. These params are got from Master Service.
     *
     * @param isTakeRemoveTopics
     * @param rmvTopicMetaConfInfoLst
     * @param sb
     * @return
     */
    @Override
    public boolean updateBrokerRemoveTopicMap(boolean isTakeRemoveTopics,
                                              List<String> rmvTopicMetaConfInfoLst,
                                              final StringBuilder sb) {
        // 该部分根据Master上的指示进行对应Topic的删除操作
        boolean needProcess = false;
        if (isTakeRemoveTopics) {
            List<String> origTopics = new ArrayList<String>();
            if (rmvTopicMetaConfInfoLst != null
                    && !rmvTopicMetaConfInfoLst.isEmpty()) {
                for (String tmpTopicMetaConfInfo : rmvTopicMetaConfInfoLst) {
                    if (TStringUtils.isBlank(tmpTopicMetaConfInfo)) {
                        continue;
                    }
                    TopicMetadata topicMetadata =
                            new TopicMetadata(brokerDefMetadata, tmpTopicMetaConfInfo);
                    if (topicMetadata.getStatusId() > TStatusConstants.STATUS_TOPIC_SOFT_DELETE) {
                        TopicMetadata tmpRemovedTopicConf =
                                removedTopicConfigMap.get(topicMetadata.getTopic());
                        if (tmpRemovedTopicConf == null) {
                            removedTopicConfigMap.put(topicMetadata.getTopic(), topicMetadata);
                        }
                        needProcess = true;
                    }
                    origTopics.add(topicMetadata.getTopic());
                }
            }
            List<String> tmpTopics = new ArrayList<String>();
            for (Map.Entry<String, TopicMetadata> entry : removedTopicConfigMap.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }
                if ((!origTopics.contains(entry.getKey()))
                        && (entry.getValue().getStatusId() > TStatusConstants.STATUS_TOPIC_SOFT_REMOVE)) {
                    tmpTopics.add(entry.getKey());
                }
            }
            if (!tmpTopics.isEmpty()) {
                for (String tmpTopic : tmpTopics) {
                    TopicMetadata topicMetadata = removedTopicConfigMap.get(tmpTopic);
                    if (topicMetadata == null) {
                        continue;
                    }
                    if (topicMetadata.getStatusId() > TStatusConstants.STATUS_TOPIC_SOFT_REMOVE) {
                        removedTopicConfigMap.remove(tmpTopic);
                        logger.info(sb
                                .append("[Metadata Manage] Master removed topic, the broker sync removes topic ")
                                .append(tmpTopic).toString());
                        sb.delete(0, sb.length());
                    }
                }
            }
        }
        if (removedTopicConfigMap.isEmpty()) {
            needProcess = false;
        }
        return needProcess;
    }

    @Override
    public void addPropertyChangeListener(final String propertyName,
                                          final PropertyChangeListener listener) {
        this.propertyChangeSupport.addPropertyChangeListener(propertyName, listener);
    }

}
