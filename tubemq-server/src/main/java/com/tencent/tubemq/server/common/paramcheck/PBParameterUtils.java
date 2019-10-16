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

package com.tencent.tubemq.server.common.paramcheck;


import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.ConsumerInfo;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.broker.metadata.MetadataManage;
import com.tencent.tubemq.server.broker.metadata.TopicMetadata;
import com.tencent.tubemq.server.master.MasterConfig;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumeGroupSettingEntity;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.ConsumerBandInfo;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PBParameterUtils {

    private static final Logger logger = LoggerFactory.getLogger(PBParameterUtils.class);

    /**
     * Check request topic list of producer
     *
     * @param reqTopicLst the topic list to be checked.
     * @param strBuffer   a string buffer used to construct the result
     * @return the check result
     */
    public static ParamCheckResult checkProducerTopicList(final List<String> reqTopicLst,
                                                          final StringBuilder strBuffer) {
        ParamCheckResult retResult = new ParamCheckResult();
        if (reqTopicLst == null) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    "Request miss necessary topic field info!");
            return retResult;
        }
        Set<String> transTopicList = new HashSet<String>();
        if (!reqTopicLst.isEmpty()) {
            for (String topic : reqTopicLst) {
                if (TStringUtils.isNotBlank(topic)) {
                    transTopicList.add(topic.trim());
                }
            }
        }
        if (transTopicList.size() > TBaseConstants.META_MAX_BOOKED_TOPIC_COUNT) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    strBuffer.append("Booked topic's count over max value, required max count is ")
                            .append(TBaseConstants.META_MAX_BOOKED_TOPIC_COUNT).toString());
            strBuffer.delete(0, strBuffer.length());
            return retResult;
        }
        retResult.setCheckData(transTopicList);
        return retResult;
    }

    /**
     * Check request topic list of consumer
     *
     * @param reqTopicLst the topic list to be checked.
     * @param strBuffer   a string buffer used to construct the result
     * @return the check result
     */
    public static ParamCheckResult checkConsumerTopicList(final List<String> reqTopicLst,
                                                          final StringBuilder strBuffer) {
        ParamCheckResult retResult = new ParamCheckResult();
        if ((reqTopicLst == null)
                || (reqTopicLst.isEmpty())) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    "Request miss necessary subscribed topicList data!");
            return retResult;
        }
        Set<String> transTopicSet = new HashSet<>();
        for (String topicItem : reqTopicLst) {
            if (TStringUtils.isBlank(topicItem)) {
                continue;
            }
            transTopicSet.add(topicItem.trim());
        }
        if (transTopicSet.isEmpty()) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    "Request subscribed topicList data must not Blank!");
            return retResult;
        }
        if (transTopicSet.size() > TBaseConstants.META_MAX_BOOKED_TOPIC_COUNT) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    strBuffer.append("Subscribed topicList size over max value, required max count is ")
                            .append(TBaseConstants.META_MAX_BOOKED_TOPIC_COUNT).toString());
            strBuffer.delete(0, strBuffer.length());
            return retResult;
        }
        retResult.setCheckData(transTopicSet);
        return retResult;
    }

    public static ParamCheckResult checkConsumerOffsetSetInfo(boolean isReqConsumeBand,
                                                              final Set<String> reqTopicSet,
                                                              final String requiredParts,
                                                              final StringBuilder strBuffer) {
        Map<String, Long> requiredPartMap = new HashMap<String, Long>();
        ParamCheckResult retResult = new ParamCheckResult();
        if (!isReqConsumeBand) {
            retResult.setCheckData(requiredPartMap);
            return retResult;
        }
        if (TStringUtils.isBlank(requiredParts)) {
            retResult.setCheckData(requiredPartMap);
            return retResult;
        }
        String[] partOffsetItems = requiredParts.trim().split(TokenConstants.ARRAY_SEP);
        for (String partOffset : partOffsetItems) {
            String[] partKeyVal = partOffset.split(TokenConstants.EQ);
            if (partKeyVal.length == 1) {
                retResult.setCheckResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        strBuffer.append("[Parameter error] unformatted Partition-Offset value : ")
                                .append(partOffset).append(" must be aa:bbb:ccc=val1,ddd:eee:ff=val2").toString());
                return retResult;
            }
            String[] partKeyItems = partKeyVal[0].trim().split(TokenConstants.ATTR_SEP);
            if (partKeyItems.length != 3) {
                retResult.setCheckResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        strBuffer.append("[Parameter error] unformatted Partition-Offset value : ")
                                .append(partOffset).append(" must be aa:bbb:ccc=val1,ddd:eee:ff=val2").toString());
                return retResult;
            }
            if (!reqTopicSet.contains(partKeyItems[1].trim())) {
                retResult.setCheckResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        strBuffer.append("[Parameter error] wrong offset reset for unsubscribed topic: reset item is ")
                                .append(partOffset).append(", request topicList are ")
                                .append(reqTopicSet.toString()).toString());
                return retResult;
            }
            try {
                requiredPartMap.put(partKeyVal[0].trim(), Long.parseLong(partKeyVal[1].trim()));
            } catch (Throwable ex) {
                retResult.setCheckResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        strBuffer.append("[Parameter error] required long type value of ")
                                .append(partOffset).append("' Offset!").toString());
                return retResult;
            }
        }
        retResult.setCheckData(requiredPartMap);
        return retResult;
    }

    public static ParamCheckResult checkConsumerInputInfo(ConsumerInfo inConsumerInfo,
                                                          final MasterConfig masterConfig,
                                                          final BrokerConfManage defaultBrokerConfManage,
                                                          final TopicPSInfoManager topicPSInfoManager,
                                                          final StringBuilder strBuffer) throws Exception {
        ParamCheckResult retResult = new ParamCheckResult();
        if (!inConsumerInfo.isRequireBound()) {
            retResult.setCheckData(inConsumerInfo);
            return retResult;
        }
        if (TStringUtils.isBlank(inConsumerInfo.getSessionKey())) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    "[Parameter error] blank value of sessionKey!");
            return retResult;
        }
        inConsumerInfo.setSessionKey(inConsumerInfo.getSessionKey().trim());
        if (inConsumerInfo.getSourceCount() <= 0) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    "[Parameter error] totalSourceCount must over zero!");
            return retResult;
        }
        BdbConsumeGroupSettingEntity offsetResetGroupEntity =
                defaultBrokerConfManage.getBdbConsumeGroupSetting(inConsumerInfo.getGroup());
        if (masterConfig.isStartOffsetResetCheck()) {
            if ((offsetResetGroupEntity == null)
                    || (offsetResetGroupEntity.getEnableBind() != 1)) {
                if (offsetResetGroupEntity == null) {
                    retResult.setCheckResult(false,
                            TErrCodeConstants.BAD_REQUEST,
                            "[unauthorized subscribe] ConsumeGroup must be authorized by administrator before" +
                                    " using bound subscribe, please contact to administrator!");
                } else {
                    retResult.setCheckResult(false,
                            TErrCodeConstants.BAD_REQUEST,
                            "[unauthorized subscribe] ConsumeGroup's authorization status is not enable for" +
                                    " using bound subscribe, please contact to administrator!");
                }
                return retResult;
            }
            Date currentDate = new Date();
            Date lastDate = offsetResetGroupEntity.getLastBindUsedDate();
            if (lastDate == null
                    || (lastDate.before(currentDate)
                    && (int) ((lastDate.getTime() - currentDate.getTime()) / (1000 * 3600 * 8)) > 1)) {
                defaultBrokerConfManage.confUpdBdbConsumeGroupLastUsedTime(inConsumerInfo.getGroup());
            }
        }
        int allowRate = (offsetResetGroupEntity != null
                && offsetResetGroupEntity.getAllowedBrokerClientRate() > 0)
                ? offsetResetGroupEntity.getAllowedBrokerClientRate() : masterConfig.getMaxGroupBrokerConsumeRate();
        int maxBrokerCount = topicPSInfoManager.getTopicMaxBrokerCount(inConsumerInfo.getTopicSet());
        int curBClientRate = (int) Math.floor(maxBrokerCount / inConsumerInfo.getSourceCount());
        if (curBClientRate > allowRate) {
            int minClientCnt = (int) (maxBrokerCount / allowRate);
            if (maxBrokerCount % allowRate != 0) {
                minClientCnt += 1;
            }
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    strBuffer.append("[Parameter error] System requires at least ")
                            .append(minClientCnt).append(" clients to consume data together, please add client" +
                            " resources!").toString());
            return retResult;
        }
        retResult.setCheckData(inConsumerInfo);
        return retResult;
    }

    // #lizard forgives
    public static ParamCheckResult validConsumerExistInfo(ConsumerInfo inConsumerInfo,
                                                          boolean isSelectBig,
                                                          ConsumerBandInfo consumerBandInfo,
                                                          final StringBuilder strBuffer) throws Exception {
        // 该部分处理主要是检查新接入的客户端是否与已存在的消费端消费目标一致,
        // 包括是否绑定消费,绑定消费的参数是否一致,订阅的topic集合是否一致,订阅的过滤消费项集合是否一致
        ParamCheckResult retResult = new ParamCheckResult();
        if (consumerBandInfo == null) {
            retResult.setCheckData(inConsumerInfo);
            return retResult;
        }
        // 确认消费行为是不是一致
        if (inConsumerInfo.isRequireBound() != consumerBandInfo.isBandConsume()) {
            if (inConsumerInfo.isRequireBound()) {
                strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                        .append(" using bound subscribe is inconsistency with other consumers using unbound" +
                                " subscribe in the group");
            } else {
                strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                        .append(" using unbound subscribe is inconsistency with other consumers using bound" +
                                " subscribe in the group");
            }
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    strBuffer.toString());
            logger.warn(strBuffer.toString());
            return retResult;
        }
        // 先检查消费的topic集合
        List<ConsumerInfo> infoList = consumerBandInfo.getConsumerInfoList();
        Set<String> existedTopics = consumerBandInfo.getTopicSet();
        Map<String, TreeSet<String>> existedTopicCondtions = consumerBandInfo.getTopicConditions();
        if (existedTopics != null && !existedTopics.isEmpty()) {
            if (existedTopics.size() != inConsumerInfo.getTopicSet().size()
                    || !existedTopics.containsAll(inConsumerInfo.getTopicSet())) {

                retResult.setCheckResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                                .append(" subscribed topics ").append(inConsumerInfo.getTopicSet())
                                .append(" is inconsistency with other consumers in the group, existedTopics: ")
                                .append(existedTopics).toString());
                logger.warn(strBuffer.toString());
                return retResult;
            }
        }
        if (infoList != null && !infoList.isEmpty()) {
            boolean isCondEqual = true;
            if (existedTopicCondtions == null || existedTopicCondtions.isEmpty()) {
                if (inConsumerInfo.getTopicConditions().isEmpty()) {
                    isCondEqual = true;
                } else {
                    isCondEqual = false;
                    strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                            .append(" subscribe with filter condition ")
                            .append(inConsumerInfo.getTopicConditions())
                            .append(" is inconsistency with other consumers in the group: topic without conditions");
                }
            } else {
                // 该部分主要是做topic的过滤条件比较
                if (inConsumerInfo.getTopicConditions().isEmpty()) {
                    isCondEqual = false;
                    strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                            .append(" subscribe without filter condition ")
                            .append(" is inconsistency with other consumers in the group, existed topic conditions is ")
                            .append(existedTopicCondtions);
                } else {
                    Set<String> existedCondTopics = existedTopicCondtions.keySet();
                    Set<String> reqCondTopics = inConsumerInfo.getTopicConditions().keySet();
                    if (existedCondTopics.size() != reqCondTopics.size()
                            || !existedCondTopics.containsAll(reqCondTopics)) {
                        isCondEqual = false;
                        strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                                .append(" subscribe with filter condition ")
                                .append(inConsumerInfo.getTopicConditions())
                                .append(" is inconsistency with other consumers in the group, existed topic" +
                                        " conditions is ")
                                .append(existedTopicCondtions);
                    } else {
                        isCondEqual = true;
                        for (String topicKey : existedCondTopics) {
                            if ((existedTopicCondtions.get(topicKey).size()
                                    != inConsumerInfo.getTopicConditions().get(topicKey).size())
                                    || (!existedTopicCondtions.get(topicKey).containsAll(inConsumerInfo
                                    .getTopicConditions().get(topicKey)))) {
                                isCondEqual = false;
                                strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                                        .append(" subscribe with filter condition ")
                                        .append(inConsumerInfo.getTopicConditions())
                                        .append(" is inconsistency with other consumers in the group, existed topic" +
                                                " conditions is ")
                                        .append(existedTopicCondtions);
                                break;
                            }
                        }
                    }
                }
            }
            if (!isCondEqual) {
                retResult.setCheckResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        strBuffer.toString());
                logger.warn(strBuffer.toString());
                return retResult;
            }
        }
        if (inConsumerInfo.isRequireBound()) {
            // 如果sessionKey不一致,则表示上一轮的消费还没完全退出
            // 为了避免offset设置不完全,需要完全清理之上的数据后才进行本轮次的消费重置及消费
            if (!inConsumerInfo.getSessionKey().equals(consumerBandInfo.getSessionKey())) {
                strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                        .append("'s sessionKey is inconsistency with other consumers in the group, required is ")
                        .append(consumerBandInfo.getSessionKey()).append(", request is ")
                        .append(inConsumerInfo.getSessionKey());
                retResult.setCheckResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        strBuffer.toString());
                logger.warn(strBuffer.toString());
                return retResult;
            }
            // 选择offset的偏好也要保持一致
            if (isSelectBig != consumerBandInfo.isSelectedBig()) {
                strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                        .append("'s isSelectBig is inconsistency with other consumers in the group, required is ")
                        .append(consumerBandInfo.isSelectedBig())
                        .append(", request is ").append(isSelectBig);
                retResult.setCheckResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        strBuffer.toString());
                logger.warn(strBuffer.toString());
                return retResult;
            }
            // 启动最少的消费者量也要一致,如果不一致有些offset就无法作设置,形成数据消费丢的情况
            if (inConsumerInfo.getSourceCount() != consumerBandInfo.getSourceCount()) {
                strBuffer.append("[Inconsistency subscribe] ").append(inConsumerInfo.getConsumerId())
                        .append("'s sourceCount is inconsistency with other consumers in the group, required is ")
                        .append(consumerBandInfo.getSourceCount())
                        .append(", request is ").append(inConsumerInfo.getSourceCount());
                retResult.setCheckResult(false,
                        TErrCodeConstants.BAD_REQUEST,
                        strBuffer.toString());
                logger.warn(strBuffer.toString());
                return retResult;
            }
        }
        boolean registered = false;
        if (infoList != null) {
            for (ConsumerInfo info : infoList) {
                if (info.getConsumerId().equals(inConsumerInfo.getConsumerId())) {
                    registered = true;
                }
            }
        }
        retResult.setCheckData(registered);
        return retResult;
    }

    /**
     * Check the id of broker
     *
     * @param brokerId  the id of broker to be checked
     * @param strBuffer the string buffer used to construct check result
     * @return the check result
     */
    public static ParamCheckResult checkBrokerId(final String brokerId,
                                                 final StringBuilder strBuffer) {
        ParamCheckResult retResult = new ParamCheckResult();
        if (TStringUtils.isBlank(brokerId)) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    "Request miss necessary brokerId data");
            return retResult;
        }
        String tmpValue = brokerId.trim();
        try {
            Integer.parseInt(tmpValue);
        } catch (Throwable e) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    strBuffer.append("Parse brokerId to int failure ").append(e.getMessage()).toString());
            strBuffer.delete(0, strBuffer.length());
            return retResult;
        }
        retResult.setCheckData(tmpValue);
        return retResult;
    }

    /**
     * Check the topic name.
     *
     * @param topicName      the topic name to check
     * @param metadataManage the metadata manager which contains topic information
     * @param strBuffer      the string buffer used to construct the check result
     * @return the check result
     */
    public static ParamCheckResult checkConsumeTopicName(final String topicName,
                                                         final MetadataManage metadataManage,
                                                         final StringBuilder strBuffer) {
        ParamCheckResult retResult = new ParamCheckResult();
        if (TStringUtils.isBlank(topicName)) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    "Request miss necessary topicName data!");
            return retResult;
        }
        String tmpValue = topicName.trim();
        if (metadataManage.getTopicMetadata(tmpValue) == null) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.FORBIDDEN,
                    strBuffer.append("Topic ").append(tmpValue)
                            .append(" not existed, please check your configure").toString());
            return retResult;
        }
        retResult.setCheckData(tmpValue);
        return retResult;
    }

    /**
     * Check the existing topic name info
     *
     * @param topicName      the topic name to be checked.
     * @param partitionId    the partition ID where the topic locates
     * @param metadataManage the metadata manager which contains topic information
     * @param strBuffer      the string buffer used to construct the check result
     * @return the check result
     */
    public static ParamCheckResult checkExistTopicNameInfo(final String topicName,
                                                           final int partitionId,
                                                           final MetadataManage metadataManage,
                                                           final StringBuilder strBuffer) {
        ParamCheckResult retResult = new ParamCheckResult();
        if (TStringUtils.isBlank(topicName)) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    "Request miss necessary topicName data!");
            return retResult;
        }
        String tmpValue = topicName.trim();
        TopicMetadata topicMetadata = metadataManage.getTopicMetadata(tmpValue);
        if (topicMetadata == null) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.FORBIDDEN,
                    strBuffer.append("Topic ").append(tmpValue)
                            .append(" not existed, please check your configure").toString());
            return retResult;
        }
        if (metadataManage.isClosedTopic(tmpValue)) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.FORBIDDEN,
                    strBuffer.append("Topic ").append(tmpValue).append(" has been closed").toString());
            return retResult;
        }
        int realPartition = partitionId < TBaseConstants.META_STORE_INS_BASE
                ? partitionId : partitionId % TBaseConstants.META_STORE_INS_BASE;
        if ((realPartition < 0) || (realPartition >= topicMetadata.getNumPartitions())) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.FORBIDDEN,
                    strBuffer.append("Partition ")
                            .append(tmpValue).append("-").append(partitionId)
                            .append(" not existed, please check your configure").toString());
            return retResult;
        }
        retResult.setCheckData(tmpValue);
        return retResult;
    }

    /**
     * Check the clientID.
     *
     * @param clientId  the client id to be checked
     * @param strBuffer the string used to construct the result
     * @return the check result
     */
    public static ParamCheckResult checkClientId(final String clientId, final StringBuilder strBuffer) {
        return validStringParameter("clientId",
                clientId, TBaseConstants.META_MAX_CLIENT_ID_LENGTH, strBuffer);
    }

    /**
     * Check the hostname.
     *
     * @param hostName  the hostname to be checked.
     * @param strBuffer the string used to construct the result
     * @return the check result
     */
    public static ParamCheckResult checkHostName(final String hostName, final StringBuilder strBuffer) {
        return validStringParameter("hostName",
                hostName, TBaseConstants.META_MAX_CLIENT_HOSTNAME_LENGTH, strBuffer);
    }

    /**
     * Check the group name
     *
     * @param groupName the group name to be checked
     * @param strBuffer the string used to construct the result
     * @return the check result
     */
    public static ParamCheckResult checkGroupName(final String groupName, final StringBuilder strBuffer) {
        return validStringParameter("groupName",
                groupName, TBaseConstants.META_MAX_GROUPNAME_LENGTH, strBuffer);
    }

    private static ParamCheckResult validStringParameter(final String paramName,
                                                         final String paramValue,
                                                         int paramMaxLen,
                                                         final StringBuilder strBuffer) {
        ParamCheckResult retResult = new ParamCheckResult();
        if (TStringUtils.isBlank(paramValue)) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    strBuffer.append("Request miss necessary ")
                            .append(paramName).append(" data!").toString());
            strBuffer.delete(0, strBuffer.length());
            return retResult;
        }
        String tmpValue = paramValue.trim();
        if (tmpValue.length() > paramMaxLen) {
            retResult.setCheckResult(false,
                    TErrCodeConstants.BAD_REQUEST,
                    strBuffer.append(paramName)
                            .append("'s length over max value, required max length is ")
                            .append(paramMaxLen).toString());
            strBuffer.delete(0, strBuffer.length());
            return retResult;
        }
        retResult.setCheckData(tmpValue);
        return retResult;
    }
}
