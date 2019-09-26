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

package com.tencent.tubemq.server.master.web.action.screen;

import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.ConsumerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.cluster.ProducerInfo;
import com.tencent.tubemq.corebase.cluster.TopicInfo;
import com.tencent.tubemq.corebase.utils.ConcurrentHashSet;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corerpc.exception.StandbyException;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.ConsumerInfoHolder;
import com.tencent.tubemq.server.master.web.simplemvc.Action;
import com.tencent.tubemq.server.master.web.simplemvc.RequestContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;


public class Master implements Action {

    private TMaster master;

    public Master(TMaster master) {
        this.master = master;
    }

    @Override
    public void execute(RequestContext requestContext) {
        StringBuilder sBuilder = new StringBuilder(512);
        try {
            HttpServletRequest req = requestContext.getReq();
            if (this.master.isStopped()) {
                throw new Exception("Sever is stopping...");
            }
            BrokerConfManage brokerConfManage =
                    this.master.getMasterTopicManage();
            if (!brokerConfManage.isSelfMaster()) {
                throw new StandbyException("Please send your request to the master Node.");
            }
            String type = req.getParameter("type");
            if ("consumer".equals(type)) {
                sBuilder = getConsumerListInfo(req, sBuilder);
            } else if ("sub_info".equals(type)) {
                sBuilder = getConsumerSubInfo(req, sBuilder);
            } else if ("producer".equals(type)) {
                sBuilder = getProducerListInfo(req, sBuilder);
            } else if ("broker".equals(type)) {
                sBuilder = innGetBrokerInfo(req, sBuilder, true);
            } else if ("newBroker".equals(type)) {
                sBuilder = innGetBrokerInfo(req, sBuilder, false);
            } else if ("topic_pub".equals(type)) {
                sBuilder = getTopicPubInfo(req, sBuilder);
            } else if ("unbalance_group".equals(type)) {
                sBuilder = getUnbalanceGroupInfo(sBuilder);
            } else {
                sBuilder.append("Unsupported request type : ").append(type);
            }
            requestContext.put("sb", sBuilder.toString());
        } catch (Exception e) {
            requestContext.put("sb", "Bad request from client. " + e.getMessage());
        }
    }

    /**
     * Get consumer list info
     *
     * @param req
     * @param sBuilder
     * @return
     */
    private StringBuilder getConsumerListInfo(final HttpServletRequest req, StringBuilder sBuilder) {
        ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
        String group = req.getParameter("group");
        if (group != null) {
            List<ConsumerInfo> consumerList = consumerHolder.getConsumerList(group);
            int index = 1;
            if (consumerList != null && !consumerList.isEmpty()) {
                Collections.sort(consumerList);
                for (ConsumerInfo consumer : consumerList) {
                    sBuilder.append(index).append(". ").append(consumer.toString()).append("\n");
                    index++;
                }
            } else {
                sBuilder.append("No such group.\n\nCurrent all groups");
                List<String> groupList = consumerHolder.getAllGroup();
                sBuilder.append("(").append(groupList.size()).append("):\n");
                for (String currGroup : groupList) {
                    sBuilder.append(currGroup).append("\n");
                }
            }
        }
        return sBuilder;
    }

    /**
     * Get consumer subscription info
     *
     * @param req
     * @param sBuilder
     * @return
     */
    private StringBuilder getConsumerSubInfo(final HttpServletRequest req, StringBuilder sBuilder) {
        ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
        String group = req.getParameter("group");
        if (group != null) {
            List<ConsumerInfo> consumerList = consumerHolder.getConsumerList(group);
            if (consumerList != null && !consumerList.isEmpty()) {
                Collections.sort(consumerList);
                sBuilder.append("\n########################## Subscribe Relationship ############################\n\n");
                Map<String, Map<String, Map<String, Partition>>> currentSubInfoMap =
                        master.getCurrentSubInfoMap();
                for (int i = 0; i < consumerList.size(); i++) {
                    ConsumerInfo consumer = consumerList.get(i);
                    sBuilder.append("*************** ").append(i + 1)
                            .append(". ").append(consumer.getConsumerId())
                            .append("#isOverTLS=").append(consumer.isOverTLS())
                            .append(" ***************");
                    Map<String, Map<String, Partition>> topicSubMap =
                            currentSubInfoMap.get(consumer.getConsumerId());
                    if (topicSubMap != null) {
                        int totalSize = 0;
                        for (Map.Entry<String, Map<String, Partition>> entry : topicSubMap.entrySet()) {
                            totalSize += entry.getValue().size();
                        }
                        sBuilder.append("(").append(totalSize).append(")\n\n");
                        for (Map.Entry<String, Map<String, Partition>> entry : topicSubMap.entrySet()) {
                            Map<String, Partition> partMap = entry.getValue();
                            if (partMap != null) {
                                for (Partition part : partMap.values()) {
                                    sBuilder.append(consumer.getConsumerId())
                                            .append("#").append(part.toString()).append("\n");
                                }
                            }
                        }
                    }
                    sBuilder.append("\n\n");
                }
            } else {
                sBuilder.append("No such group.\n\nCurrent all group");
                List<String> groupList = consumerHolder.getAllGroup();
                sBuilder.append("(").append(groupList.size()).append("):\n");
                for (String currGroup : groupList) {
                    sBuilder.append(currGroup).append("\n");
                }
            }
        }
        return sBuilder;
    }

    /**
     * Get producer list info
     *
     * @param req
     * @param sBuilder
     * @return
     */
    private StringBuilder getProducerListInfo(final HttpServletRequest req, StringBuilder sBuilder) {
        String producerId = req.getParameter("id");
        if (producerId != null) {
            ProducerInfo producer = master.getProducerHolder().getProducerInfo(producerId);
            if (producer != null) {
                sBuilder.append(producer.toString());
            } else {
                sBuilder.append("No such producer!");
            }
        } else {
            String topic = req.getParameter("topic");
            if (topic != null) {
                TopicPSInfoManager topicPSInfoManager =
                        master.getTopicPSInfoManager();
                ConcurrentHashSet<String> producerSet =
                        topicPSInfoManager.getTopicPubInfo(topic);
                if (producerSet != null && !producerSet.isEmpty()) {
                    int index = 1;
                    for (String producer : producerSet) {
                        sBuilder.append(index).append(". ").append(producer).append("\n");
                        index++;
                    }
                }
            }
        }
        return sBuilder;
    }

    /**
     * Get broker info
     *
     * @param req
     * @param sBuilder
     * @param isOldRet
     * @return
     */
    private StringBuilder innGetBrokerInfo(final HttpServletRequest req,
                                           StringBuilder sBuilder, boolean isOldRet) {
        Map<Integer, BrokerInfo> brokerInfoMap = null;
        String brokerIds = req.getParameter("ids");
        if (TStringUtils.isBlank(brokerIds)) {
            brokerInfoMap = master.getBrokerHolder().getBrokerInfoMap();
        } else {
            String[] brokerIdArr = brokerIds.split(",");
            List<Integer> idList = new ArrayList<Integer>(brokerIdArr.length);
            for (String strId : brokerIdArr) {
                idList.add(Integer.parseInt(strId));
            }
            brokerInfoMap = master.getBrokerHolder().getBrokerInfos(idList);
        }
        if (brokerInfoMap != null) {
            int index = 1;
            for (BrokerInfo broker : brokerInfoMap.values()) {
                sBuilder.append("\n################################## ")
                        .append(index).append(". ").append(broker.toString())
                        .append(" ##################################\n");
                TopicPSInfoManager topicPSInfoManager = master.getTopicPSInfoManager();
                List<TopicInfo> topicInfoList = topicPSInfoManager.getBrokerPubInfoList(broker);
                ConcurrentHashMap<String, BdbTopicConfEntity> topicConfigMap =
                        master.getMasterTopicManage().getBrokerTopicConfEntitySet(broker.getBrokerId());
                if (topicConfigMap == null) {
                    for (TopicInfo info : topicInfoList) {
                        sBuilder = info.toStrBuilderString(sBuilder);
                        sBuilder.append("\n");

                    }
                } else {
                    for (TopicInfo info : topicInfoList) {
                        BdbTopicConfEntity bdbEntity = topicConfigMap.get(info.getTopic());
                        if (bdbEntity == null) {
                            sBuilder = info.toStrBuilderString(sBuilder);
                            sBuilder.append("\n");
                        } else {
                            if (isOldRet) {
                                if (bdbEntity.isValidTopicStatus()) {
                                    sBuilder = info.toStrBuilderString(sBuilder);
                                    sBuilder.append("\n");
                                }
                            } else {
                                sBuilder = info.toStrBuilderString(sBuilder);
                                sBuilder.append(TokenConstants.SEGMENT_SEP)
                                        .append(bdbEntity.getTopicStatusId()).append("\n");
                            }
                        }
                    }
                }
                index++;
            }
        }
        return sBuilder;
    }

    /**
     * Get topic publish info
     *
     * @param req
     * @param sBuilder
     * @return
     */
    private StringBuilder getTopicPubInfo(final HttpServletRequest req, StringBuilder sBuilder) {
        String topic = req.getParameter("topic");
        Set<String> producerIds = master.getTopicPSInfoManager().getTopicPubInfo(topic);
        if (producerIds != null && !producerIds.isEmpty()) {
            for (String producerId : producerIds) {
                sBuilder.append(producerId).append("\n");
            }
        } else {
            sBuilder.append("No producer has publish this topic.");
        }
        return sBuilder;
    }

    /**
     * Get un-balanced group info
     *
     * @param sBuilder
     * @return
     */
    private StringBuilder getUnbalanceGroupInfo(StringBuilder sBuilder) {
        ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
        TopicPSInfoManager topicPSInfoManager = master.getTopicPSInfoManager();
        Map<String, Map<String, Map<String, Partition>>> currentSubInfoMap =
                master.getCurrentSubInfoMap();
        List<String> groupList = consumerHolder.getAllGroup();
        for (String group : groupList) {
            Set<String> topicSet = consumerHolder.getGroupTopicSet(group);
            for (String topic : topicSet) {
                int currPartSize = 0;
                List<ConsumerInfo> consumerList = consumerHolder.getConsumerList(group);
                for (ConsumerInfo consumer : consumerList) {
                    Map<String, Map<String, Partition>> consumerSubInfoMap =
                            currentSubInfoMap.get(consumer.getConsumerId());
                    if (consumerSubInfoMap != null) {
                        Map<String, Partition> topicSubInfoMap =
                                consumerSubInfoMap.get(topic);
                        if (topicSubInfoMap != null) {
                            currPartSize += topicSubInfoMap.size();
                        }
                    }
                }
                List<Partition> partList = topicPSInfoManager.getPartitionList(topic);
                if (currPartSize != partList.size()) {
                    sBuilder.append(group).append(":").append(topic).append("\n");
                }
            }
        }
        return sBuilder;
    }
}
