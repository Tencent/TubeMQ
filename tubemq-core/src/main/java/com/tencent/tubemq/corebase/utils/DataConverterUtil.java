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

package com.tencent.tubemq.corebase.utils;

import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.MessageExt;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.cluster.SubscribeInfo;
import com.tencent.tubemq.corebase.cluster.TopicInfo;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tube meta info converter tools
 */

public class DataConverterUtil {

    /**
     * convert string info to @link SubscribeInfo
     *
     * @param strSubInfoList return a list of SubscribeInfos
     */
    public static List<SubscribeInfo> convertSubInfo(List<String> strSubInfoList) {
        List<SubscribeInfo> subInfoList = new ArrayList<SubscribeInfo>();
        if (strSubInfoList != null) {
            for (String strSubInfo : strSubInfoList) {
                if (TStringUtils.isNotBlank(strSubInfo)) {
                    SubscribeInfo subInfo = new SubscribeInfo(strSubInfo);
                    subInfoList.add(subInfo);
                }
            }
        }
        return subInfoList;
    }

    /**
     * format SubscribeInfo to String info the reverse operator of convertSubInfo
     *
     * @param subInfoList return a list of String SubscribeInfos
     */
    public static List<String> formatSubInfo(List<SubscribeInfo> subInfoList) {
        List<String> strSubInfoList = new ArrayList<String>();
        if ((subInfoList != null) && (!subInfoList.isEmpty())) {
            for (SubscribeInfo subInfo : subInfoList) {
                if (subInfo != null) {
                    strSubInfoList.add(subInfo.toString());
                }
            }
        }
        return strSubInfoList;
    }

    /**
     * convert string info to @link Partition
     *
     * @param strPartInfoList
     * @return return a list of Partition
     */
    public static List<Partition> convertPartitionInfo(List<String> strPartInfoList) {
        List<Partition> partList = new ArrayList<Partition>();
        if (strPartInfoList != null) {
            for (String partInfo : strPartInfoList) {
                if (partInfo != null) {
                    partList.add(new Partition(partInfo));
                }
            }
        }
        return partList;
    }

    /**
     * convert string info with a brokerInfo list to @link TopicInfo
     *
     * @param brokerInfoMap
     * @param strTopicInfos return a list of TopicInfo
     */
    public static List<TopicInfo> convertTopicInfo(Map<Integer, BrokerInfo> brokerInfoMap,
                                                   List<String> strTopicInfos) {
        List<TopicInfo> topicList = new ArrayList<TopicInfo>();
        if (strTopicInfos != null) {
            for (String info : strTopicInfos) {
                if (info != null) {
                    info = info.trim();
                    String[] strInfo = info.split(TokenConstants.SEGMENT_SEP);
                    String[] strTopicInfoSet = strInfo[1].split(TokenConstants.ARRAY_SEP);
                    for (int i = 0; i < strTopicInfoSet.length; i++) {
                        String[] strTopicInfo =
                                strTopicInfoSet[i].split(TokenConstants.ATTR_SEP);
                        BrokerInfo brokerInfo =
                                brokerInfoMap.get(Integer.parseInt(strTopicInfo[0]));
                        if (brokerInfo != null) {
                            topicList.add(new TopicInfo(brokerInfo,
                                    strInfo[0], Integer.parseInt(strTopicInfo[1]),
                                    Integer.parseInt(strTopicInfo[2]), true, true));
                        }
                    }
                }
            }
        }
        return topicList;
    }

    /*********
     * convert string info to @link BrokerInfo
     *
     * @param strBrokerInfos return a BrokerInfo Map
     */
    public static Map<Integer, BrokerInfo> convertBrokerInfo(List<String> strBrokerInfos) {
        Map<Integer, BrokerInfo> brokerInfoMap =
                new ConcurrentHashMap<Integer, BrokerInfo>();
        if (strBrokerInfos != null) {
            for (String info : strBrokerInfos) {
                if (info != null) {
                    BrokerInfo brokerInfo =
                            new BrokerInfo(info, TBaseConstants.META_DEFAULT_BROKER_PORT);
                    brokerInfoMap.put(brokerInfo.getBrokerId(), brokerInfo);
                }
            }
        }
        return brokerInfoMap;
    }


    /*********
     * convert string info to  a map of TopicCondition TreeSet
     *
     * @param strTopicConditions return a map of TopicCondition TreeSet
     */
    public static Map<String, TreeSet<String>> convertTopicConditions(
            final List<String> strTopicConditions) {
        Map<String, TreeSet<String>> topicConditions =
                new HashMap<String, TreeSet<String>>();
        if (strTopicConditions == null || strTopicConditions.isEmpty()) {
            return topicConditions;
        }
        for (String topicCond : strTopicConditions) {
            if (TStringUtils.isBlank(topicCond)) {
                continue;
            }
            String[] strInfo = topicCond.split(TokenConstants.SEGMENT_SEP);
            if (TStringUtils.isBlank(strInfo[0])
                    || TStringUtils.isBlank(strInfo[1])) {
                continue;
            }
            String topicName = strInfo[0].trim();
            String[] strCondInfo = strInfo[1].split(TokenConstants.ARRAY_SEP);
            TreeSet<String> conditionSet = topicConditions.get(topicName);
            if (conditionSet == null) {
                conditionSet = new TreeSet<String>();
                topicConditions.put(topicName, conditionSet);
            }
            for (String cond : strCondInfo) {
                if (TStringUtils.isNotBlank(cond)) {
                    conditionSet.add(cond.trim());
                }
            }
        }
        return topicConditions;
    }


    /**
     * convert a list of @link ClientBroker.TransferedMessage with topicName
     * to a list of @link Message
     *
     * @param topicName
     * @param transferedMessageList return a list of @link Message
     */
    public static List<Message> convertMessage(final String topicName,
                                               List<ClientBroker.TransferedMessage> transferedMessageList) {
        List<Message> messageList = new ArrayList<Message>();
        if (transferedMessageList == null || transferedMessageList.isEmpty()) {
            return messageList;
        }
        for (ClientBroker.TransferedMessage trsMessage : transferedMessageList) {
            final int flag = trsMessage.getFlag();
            int dataCheckSum = trsMessage.getCheckSum();
            final ByteBuffer payloadData = ByteBuffer.allocate(trsMessage.getPayLoadData().size());
            payloadData.put(trsMessage.getPayLoadData().toByteArray());
            payloadData.flip();
            int payloadDataLen = payloadData.array().length;
            int currentChecksum = CheckSum.crc32(payloadData.array());
            if (dataCheckSum != currentChecksum) {
                continue;
            }
            int readPos = 0;
            String attribute = null;
            if (MessageFlagUtils.hasAttribute(flag)) {
                if (payloadDataLen < 4) {
                    continue;
                }
                final int attrLen = payloadData.getInt(0);
                payloadDataLen -= 4;
                readPos += 4;
                if (attrLen > payloadDataLen) {
                    continue;
                }
                if (attrLen > 0) {
                    final byte[] attrData = new byte[attrLen];
                    System.arraycopy(payloadData.array(), readPos, attrData, 0, attrLen);
                    try {
                        attribute = new String(attrData, TBaseConstants.META_DEFAULT_CHARSET_NAME);
                    } catch (final UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                    readPos += attrLen;
                    payloadDataLen -= attrLen;
                }
            }
            final byte[] payload = new byte[payloadDataLen];
            System.arraycopy(payloadData.array(), readPos, payload, 0, payloadDataLen);
            messageList.add(new MessageExt(trsMessage.getMessageId(), topicName, payload, attribute, flag));
        }
        return messageList;
    }

}
