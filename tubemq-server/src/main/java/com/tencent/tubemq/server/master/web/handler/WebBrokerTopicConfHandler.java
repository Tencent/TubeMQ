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

package com.tencent.tubemq.server.master.web.handler;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.TopicInfo;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.common.TServerConstants;
import com.tencent.tubemq.server.common.TStatusConstants;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumerGroupEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicAuthControlEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerSyncStatusInfo;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebBrokerTopicConfHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(WebBrokerTopicConfHandler.class);
    private TMaster master;
    private BrokerConfManage brokerConfManage;

    /**
     * Constructor
     *
     * @param master tube master
     */
    public WebBrokerTopicConfHandler(TMaster master) {
        this.master = master;
        this.brokerConfManage = this.master.getMasterTopicManage();
    }

    /**
     * Add new topic record
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminAddTopicEntityInfo(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            // user
            String createUser =
                    WebParameterUtils.validStringParameter("createUser", req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser", req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, false, createUser);
            // date
            Date createDate =
                    WebParameterUtils.validDateParameter("createDate", req.getParameter("createDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, new Date());
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate", req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, createDate);
            // topic names
            Set<String> bathAddTopicNames =
                    WebParameterUtils.getBatchTopicNames(req.getParameter("topicName"),
                            true, false, null, strBuffer);
            // broker IDs
            Set<BdbBrokerConfEntity> bathBrokerEntitySet =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"), brokerConfManage, true,
                            strBuffer);
            List<BdbTopicConfEntity> bathAddBdbTopicEntitys = new ArrayList<BdbTopicConfEntity>();
            List<BdbTopicAuthControlEntity> bathAddBdbTopicAuthControls = new ArrayList<BdbTopicAuthControlEntity>();
            // for each topic
            for (String topicName : bathAddTopicNames) {
                BdbTopicAuthControlEntity tmpTopicAuthControl =
                        brokerConfManage.getBdbEnableAuthControlByTopicName(topicName);
                if (tmpTopicAuthControl == null) {
                    bathAddBdbTopicAuthControls
                            .add(new BdbTopicAuthControlEntity(topicName,
                                    false, createUser, createDate));
                }
            }
            // for each broker
            for (BdbBrokerConfEntity oldEntity : bathBrokerEntitySet) {
                if (oldEntity == null) {
                    continue;
                }
                if (WebParameterUtils.checkBrokerInProcessing(oldEntity.getBrokerId(), brokerConfManage, strBuffer)) {
                    throw new Exception(strBuffer.toString());
                }
                ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicEntityMap =
                        brokerConfManage.getBrokerTopicConfEntitySet(oldEntity.getBrokerId());
                if (brokerTopicEntityMap != null) {
                    for (String itemTopicName : bathAddTopicNames) {
                        BdbTopicConfEntity tmpTopicConfEntity = brokerTopicEntityMap.get(itemTopicName);
                        if (tmpTopicConfEntity != null) {
                            if (tmpTopicConfEntity.isValidTopicStatus()) {
                                throw new Exception(strBuffer.append("Topic of ").append(itemTopicName)
                                        .append(" has existed in broker topic configure by brokerId=")
                                        .append(oldEntity.getBrokerId()).toString());
                            } else {
                                throw new Exception(strBuffer.append("Topic of ").append(itemTopicName)
                                        .append(" is deleted softly in brokerId=").append(oldEntity.getBrokerId())
                                        .append(", please resume the record or hard removed first!").toString());
                            }
                        }
                    }
                }
                final int defNumTopicStores = oldEntity.getNumTopicStores();
                final int defunFlushDataHold = oldEntity.getDftUnFlushDataHold();  /* TODO: not used, could remove? */
                final int defmemCacheMsgCntInK = oldEntity.getDftMemCacheMsgCntInK();
                final int defmemCacheMsgSizeInMB = oldEntity.getDftMemCacheMsgSizeInMB();
                final int defmemCacheFlushIntvl = oldEntity.getDftMemCacheFlushIntvl();
                String deleteWhen =
                        WebParameterUtils.validDecodeStringParameter("deleteWhen",
                                req.getParameter("deleteWhen"),
                                TServerConstants.CFG_DELETEWHEN_MAX_LENGTH,
                                false, oldEntity.getDftDeleteWhen());
                String deletePolicy =
                        WebParameterUtils.validDecodeStringParameter("deletePolicy",
                                req.getParameter("deletePolicy"),
                                TServerConstants.CFG_DELETEPOLICY_MAX_LENGTH,
                                false, oldEntity.getDftDeletePolicy());
                int numPartitions =
                        WebParameterUtils.validIntDataParameter("numPartitions",
                                req.getParameter("numPartitions"),
                                false, oldEntity.getDftNumPartitions(), 1);
                int unflushThreshold =
                        WebParameterUtils.validIntDataParameter("unflushThreshold",
                                req.getParameter("unflushThreshold"),
                                false, oldEntity.getDftUnflushThreshold(), 0);
                int unflushInterval =
                        WebParameterUtils.validIntDataParameter("unflushInterval",
                                req.getParameter("unflushInterval"),
                                false, oldEntity.getDftUnflushInterval(), 1);
                boolean acceptPublish =
                        WebParameterUtils.validBooleanDataParameter("acceptPublish",
                                req.getParameter("acceptPublish"),
                                false, oldEntity.isAcceptPublish());
                boolean acceptSubscribe =
                        WebParameterUtils.validBooleanDataParameter("acceptSubscribe",
                                req.getParameter("acceptSubscribe"),
                                false, oldEntity.isAcceptSubscribe());
                int numTopicStores =
                        WebParameterUtils.validIntDataParameter("numTopicStores",
                                req.getParameter("numTopicStores"),
                                false, defNumTopicStores, 1);
                int memCacheMsgCntInK =
                        WebParameterUtils.validIntDataParameter("memCacheMsgCntInK",
                                req.getParameter("memCacheMsgCntInK"),
                                false, defmemCacheMsgCntInK, 1);
                int memCacheMsgSizeInMB =
                        WebParameterUtils.validIntDataParameter("memCacheMsgSizeInMB",
                                req.getParameter("memCacheMsgSizeInMB"),
                                false, defmemCacheMsgSizeInMB, 2);
                memCacheMsgSizeInMB = memCacheMsgSizeInMB >= 2048 ? 2048 : memCacheMsgSizeInMB;
                int memCacheFlushIntvl =
                        WebParameterUtils.validIntDataParameter("memCacheFlushIntvl",
                                req.getParameter("memCacheFlushIntvl"),
                                false, defmemCacheFlushIntvl, 4000);
                int unFlushDataHold = unflushThreshold;
                String attributes = strBuffer.append(TokenConstants.TOKEN_STORE_NUM)
                        .append(TokenConstants.EQ).append(numTopicStores)
                        .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_DATA_UNFLUSHHOLD)
                        .append(TokenConstants.EQ).append(unFlushDataHold)
                        .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_MCACHE_MSG_CNT)
                        .append(TokenConstants.EQ).append(memCacheMsgCntInK)
                        .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_MCACHE_MSG_SIZE)
                        .append(TokenConstants.EQ).append(memCacheMsgSizeInMB)
                        .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_MCACHE_FLUSH_INTVL)
                        .append(TokenConstants.EQ).append(memCacheFlushIntvl).toString();
                strBuffer.delete(0, strBuffer.length());
                for (String itemTopicName : bathAddTopicNames) {
                    bathAddBdbTopicEntitys.add(new BdbTopicConfEntity(oldEntity.getBrokerId(),
                            oldEntity.getBrokerIp(), oldEntity.getBrokerPort(), itemTopicName,
                            numPartitions, unflushThreshold, unflushInterval, deleteWhen,
                            deletePolicy, acceptPublish, acceptSubscribe, numTopicStores,
                            attributes, createUser, createDate, modifyUser, modifyDate));
                }
            }
            inAddTopicConfigInfo(bathAddBdbTopicEntitys, bathAddBdbTopicAuthControls);
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * Add new topic record in batch
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminBathAddTopicEntityInfo(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String createUser =
                    WebParameterUtils.validStringParameter("createUser", req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            Date createDate =
                    WebParameterUtils.validDateParameter("createDate", req.getParameter("createDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, new Date());
            List<Map<String, Object>> topicJsonArray =
                    WebParameterUtils.checkAndGetJsonArray("topicJsonSet",
                            req.getParameter("topicJsonSet"), TBaseConstants.META_VALUE_UNDEFINED, true);
            if ((topicJsonArray == null) || (topicJsonArray.isEmpty())) {
                throw new Exception("Null value of topicJsonSet, please set the value first!");
            }
            Set<String> bathAddTopicNames = new HashSet<String>();
            Set<String> bathAddItemKeys = new HashSet<String>();
            List<BdbTopicAuthControlEntity> bathTopicAuthInfos = new ArrayList<BdbTopicAuthControlEntity>();
            List<BdbTopicConfEntity> bathAddBdbTopicEntitys = new ArrayList<BdbTopicConfEntity>();
            for (int count = 0; count < topicJsonArray.size(); count++) {
                Map<String, Object> jsonObject = topicJsonArray.get(count);
                try {
                    int brokerId =
                            WebParameterUtils.validIntDataParameter("brokerId",
                                    jsonObject.get("brokerId"), true, 0, 1);
                    BdbBrokerConfEntity brokerConfEntity =
                            brokerConfManage.getBrokerDefaultConfigStoreInfo(brokerId);
                    if (brokerConfEntity == null) {
                        throw new Exception(strBuffer
                                .append("Not found broker default configure record by brokerId=").append(brokerId)
                                .append(", please create the broker's default configure first!").toString());
                    }
                    String topicName =
                            WebParameterUtils.validStringParameter("topicName", jsonObject.get("topicName"),
                                    TBaseConstants.META_MAX_TOPICNAME_LENGTH, true, "");
                    String inputKey = strBuffer.append(brokerId).append("-").append(topicName).toString();
                    strBuffer.delete(0, strBuffer.length());
                    if (bathAddItemKeys.contains(inputKey)) {
                        continue;
                    }
                    if (WebParameterUtils.checkBrokerInProcessing(brokerId, brokerConfManage, strBuffer)) {
                        throw new Exception(strBuffer.toString());
                    }
                    ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicEntityMap =
                            brokerConfManage.getBrokerTopicConfEntitySet(brokerConfEntity.getBrokerId());
                    if (brokerTopicEntityMap != null) {
                        BdbTopicConfEntity tmpTopicConfEntity = brokerTopicEntityMap.get(topicName);
                        if (tmpTopicConfEntity != null) {
                            if (tmpTopicConfEntity.isValidTopicStatus()) {
                                throw new Exception(strBuffer
                                        .append("Duplicate add broker's topic configure, exist record is: ")
                                        .append(tmpTopicConfEntity).toString());
                            } else {
                                throw new Exception(strBuffer.append("Topic of ").append(topicName)
                                        .append(" is deleted softly in brokerId=")
                                        .append(brokerId).append(", please resume the record or hard removed first!")
                                        .toString());
                            }
                        }
                    }
                    final String deleteWhen =
                            WebParameterUtils.validDecodeStringParameter("deleteWhen",
                                    jsonObject.get("deleteWhen"),
                                    TServerConstants.CFG_DELETEWHEN_MAX_LENGTH,
                                    false, brokerConfEntity.getDftDeleteWhen());
                    final String deletePolicy =
                            WebParameterUtils.validDecodeStringParameter("deletePolicy",
                                    jsonObject.get("deletePolicy"),
                                    TServerConstants.CFG_DELETEPOLICY_MAX_LENGTH,
                                    false, brokerConfEntity.getDftDeletePolicy());
                    final int numPartitions =
                            WebParameterUtils.validIntDataParameter("numPartitions",
                                    jsonObject.get("numPartitions"),
                                    false, brokerConfEntity.getDftNumPartitions(), 1);
                    final int unflushThreshold =
                            WebParameterUtils.validIntDataParameter("unflushThreshold",
                                    jsonObject.get("unflushThreshold"),
                                    false, brokerConfEntity.getDftUnflushThreshold(), 0);
                    final int unflushInterval =
                            WebParameterUtils.validIntDataParameter("unflushInterval",
                                    jsonObject.get("unflushInterval"),
                                    false, brokerConfEntity.getDftUnflushInterval(), 1);
                    final boolean acceptPublish =
                            WebParameterUtils.validBooleanDataParameter("acceptPublish",
                                    jsonObject.get("acceptPublish"),
                                    false, brokerConfEntity.isAcceptPublish());
                    final boolean acceptSubscribe =
                            WebParameterUtils.validBooleanDataParameter("acceptSubscribe",
                                    jsonObject.get("acceptSubscribe"),
                                    false, brokerConfEntity.isAcceptSubscribe());
                    final int numTopicStores =
                            WebParameterUtils.validIntDataParameter("numTopicStores",
                                    jsonObject.get("numTopicStores"),
                                    false, brokerConfEntity.getNumTopicStores(), 1);
                    final int memCacheMsgCntInK =
                            WebParameterUtils.validIntDataParameter("memCacheMsgCntInK",
                                    jsonObject.get("memCacheMsgCntInK"),
                                    false, brokerConfEntity.getDftMemCacheMsgCntInK(), 1);
                    int memCacheMsgSizeInMB =
                            WebParameterUtils.validIntDataParameter("memCacheMsgSizeInMB",
                                    jsonObject.get("memCacheMsgSizeInMB"),
                                    false, brokerConfEntity.getDftMemCacheMsgSizeInMB(), 2);
                    memCacheMsgSizeInMB = memCacheMsgSizeInMB >= 2048 ? 2048 : memCacheMsgSizeInMB;
                    int memCacheFlushIntvl =
                            WebParameterUtils.validIntDataParameter("memCacheFlushIntvl",
                                    jsonObject.get("memCacheFlushIntvl"),
                                    false, brokerConfEntity.getDftMemCacheFlushIntvl(), 4000);
                    int unFlushDataHold = unflushThreshold;
                    String itemCreateUser =
                            WebParameterUtils.validStringParameter("createUser",
                                    jsonObject.get("createUser"),
                                    TBaseConstants.META_MAX_USERNAME_LENGTH, false, null);
                    Date itemCreateDate =
                            WebParameterUtils.validDateParameter("createDate",
                                    jsonObject.get("createDate"),
                                    TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, null);
                    if ((TStringUtils.isBlank(itemCreateUser)) || (itemCreateDate == null)) {
                        itemCreateUser = createUser;
                        itemCreateDate = createDate;
                    }
                    String attributes =
                            strBuffer.append(TokenConstants.TOKEN_STORE_NUM)
                                    .append(TokenConstants.EQ).append(numTopicStores)
                                    .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_DATA_UNFLUSHHOLD)
                                    .append(TokenConstants.EQ).append(unFlushDataHold)
                                    .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_MCACHE_MSG_CNT)
                                    .append(TokenConstants.EQ).append(memCacheMsgCntInK)
                                    .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_MCACHE_MSG_SIZE)
                                    .append(TokenConstants.EQ).append(memCacheMsgSizeInMB)
                                    .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_MCACHE_FLUSH_INTVL)
                                    .append(TokenConstants.EQ).append(memCacheFlushIntvl).toString();
                    strBuffer.delete(0, strBuffer.length());
                    bathAddItemKeys.add(inputKey);
                    bathAddBdbTopicEntitys.add(new BdbTopicConfEntity(brokerConfEntity.getBrokerId(),
                            brokerConfEntity.getBrokerIp(), brokerConfEntity.getBrokerPort(),
                            topicName, numPartitions, unflushThreshold, unflushInterval,
                            deleteWhen, deletePolicy, acceptPublish, acceptSubscribe,
                            numTopicStores, attributes, itemCreateUser, itemCreateDate,
                            itemCreateUser, itemCreateDate));
                    if (!bathAddTopicNames.contains(topicName)) {
                        BdbTopicAuthControlEntity tmpTopicAuthControl =
                                brokerConfManage.getBdbEnableAuthControlByTopicName(topicName);
                        if (tmpTopicAuthControl == null) {
                            bathTopicAuthInfos.add(
                                    new BdbTopicAuthControlEntity(topicName, false, createUser, createDate));
                        }
                    }
                    bathAddTopicNames.add(topicName);
                } catch (Exception ee) {
                    strBuffer.delete(0, strBuffer.length());
                    throw new Exception(strBuffer.append("Process data exception, data is :")
                            .append(jsonObject.toString()).append(", exception is : ")
                            .append(ee.getMessage()).toString());
                }
            }
            inAddTopicConfigInfo(bathAddBdbTopicEntitys, bathTopicAuthInfos);
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * Private method to add topic config info
     *
     * @param bathAddBdbTopicEntitys
     * @param bathTopicAuthInfos
     * @throws Exception
     */
    private void inAddTopicConfigInfo(List<BdbTopicConfEntity> bathAddBdbTopicEntitys,
                                      List<BdbTopicAuthControlEntity> bathTopicAuthInfos) throws Exception {
        boolean inserted = false;
        try {
            for (BdbTopicConfEntity itemBdbTopicEntity : bathAddBdbTopicEntitys) {  // for each topic
                BdbBrokerConfEntity brokerConfEntity =
                        brokerConfManage.getBrokerDefaultConfigStoreInfo(itemBdbTopicEntity.getBrokerId());
                // if broker conf is not set, or the broker is busy with processing events,
                // skip this topic
                if (brokerConfEntity == null
                        || WebParameterUtils.checkBrokerInProcessing(itemBdbTopicEntity.getBrokerId(), brokerConfManage,
                        null)) {
                    continue;
                }
                // TODO: could move into "if (result)" clause?
                BrokerSyncStatusInfo brokerSyncStatusInfo =
                        brokerConfManage.getBrokerRunSyncStatusInfo(itemBdbTopicEntity.getBrokerId());
                boolean result = brokerConfManage.confAddTopicConfig(itemBdbTopicEntity);
                if (result) {  // if it succeeds in adding topic config
                    // set Fast start = false
                    if (brokerSyncStatusInfo != null) {
                        brokerSyncStatusInfo.setFastStart(false);
                    }
                    // update broker config
                    if (!brokerConfEntity.isConfDataUpdated()) {  // config data NOT updated
                        brokerConfManage.updateBrokerConfChanged(brokerConfEntity.getBrokerId(),
                                true, false);
                    }
                }
                inserted = true;
            }  // for each topic

            // if at least one topic is updated,
            // update topic authorization control
            if (inserted) {
                for (BdbTopicAuthControlEntity topicAuthControlEntity
                        : bathTopicAuthInfos) {
                    BdbTopicAuthControlEntity tmpTopicAuthControl =
                            brokerConfManage.getBdbEnableAuthControlByTopicName(topicAuthControlEntity.getTopicName());
                    if (tmpTopicAuthControl == null) {
                        brokerConfManage.confSetBdbTopicAuthControl(topicAuthControlEntity);
                    }
                }
            }
        } catch (Exception ee) {
            logger.warn("Fun.inAddTopicConfigInfo throw exception", ee);
            //
        }
    }

    /**
     * Query topic info
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminQueryTopicCfgEntityAndRunInfo(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        BdbTopicConfEntity webTopicEntity = new BdbTopicConfEntity();
        try {
            webTopicEntity
                    .setDeleteWhen(WebParameterUtils.validDecodeStringParameter("deleteWhen",
                            req.getParameter("deleteWhen"),
                            TServerConstants.CFG_DELETEWHEN_MAX_LENGTH, false, null));
            webTopicEntity
                    .setDeletePolicy(WebParameterUtils.validDecodeStringParameter("deletePolicy",
                            req.getParameter("deletePolicy"),
                            TServerConstants.CFG_DELETEPOLICY_MAX_LENGTH, false, null));
            webTopicEntity
                    .setUnflushInterval(WebParameterUtils.validIntDataParameter("unflushInterval",
                            req.getParameter("unflushInterval"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, 1));
            webTopicEntity
                    .setUnflushThreshold(WebParameterUtils.validIntDataParameter("unflushThreshold",
                            req.getParameter("unflushThreshold"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, 0));
            webTopicEntity
                    .setTopicStatusId(WebParameterUtils.validIntDataParameter("topicStatusId",
                            req.getParameter("topicStatusId"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, TStatusConstants.STATUS_TOPIC_OK));
            webTopicEntity
                    .setNumPartitions(WebParameterUtils.validIntDataParameter("numPartitions",
                            req.getParameter("numPartitions"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, 1));
            webTopicEntity
                    .setCreateUser(WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, false, null));
            webTopicEntity
                    .setModifyUser(WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, false, null));
            webTopicEntity
                    .setNumTopicStores(WebParameterUtils.validIntDataParameter("numTopicStores",
                            req.getParameter("numTopicStores"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, 1));
            webTopicEntity
                    .setMemCacheMsgSizeInMB(WebParameterUtils.validIntDataParameter("memCacheMsgSizeInMB",
                            req.getParameter("memCacheMsgSizeInMB"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, 2));
            webTopicEntity
                    .setMemCacheMsgCntInK(WebParameterUtils.validIntDataParameter("memCacheMsgCntInK",
                            req.getParameter("memCacheMsgCntInK"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, 1));
            webTopicEntity
                    .setMemCacheFlushIntvl(WebParameterUtils.validIntDataParameter("memCacheFlushIntvl",
                            req.getParameter("memCacheFlushIntvl"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, 4000));
            Set<Integer> bathBrokerIds =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"), false);
            if (bathBrokerIds.size() == 1) {
                for (Integer brokerId : bathBrokerIds) {
                    webTopicEntity.setBrokerId(brokerId);
                }
            }
            Set<String> bathOpTopicNames =
                    WebParameterUtils.getBatchTopicNames(req.getParameter("topicName"), false, false, null, strBuffer);
            if (bathOpTopicNames.size() == 1) {
                for (String topicName : bathOpTopicNames) {
                    webTopicEntity.setTopicName(topicName);
                }
            }
            ConcurrentHashMap<String, List<BdbTopicConfEntity>> queryResultMap =
                    brokerConfManage.getBdbTopicEntityMap(webTopicEntity);
            TopicPSInfoManager topicPSInfoManager = master.getTopicPSInfoManager();
            SimpleDateFormat formatter =
                    new SimpleDateFormat(TBaseConstants.META_TMP_DATE_VALUE);
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"data\":[");
            int totalCount = 0;
            for (Map.Entry<String, List<BdbTopicConfEntity>> entry : queryResultMap.entrySet()) {
                if ((!bathOpTopicNames.isEmpty()) && (!bathOpTopicNames.contains(entry.getKey()))) {
                    continue;
                }
                if (totalCount++ > 0) {
                    strBuffer.append(",");
                }
                int totalCfgNumPartCount = 0;
                int totalRunNumPartCount = 0;
                boolean isSrvAcceptPublish = false;
                boolean isSrvAcceptSubscribe = false;
                boolean isAcceptPublish = false;
                boolean isAcceptSubscribe = false;
                strBuffer.append("{\"topicName\":\"").append(entry.getKey()).append("\",\"topicInfo\":[");
                int count = 0;
                for (BdbTopicConfEntity entity : entry.getValue()) {
                    if ((!bathBrokerIds.isEmpty()) && (!bathBrokerIds.contains(entity.getBrokerId()))) {
                        continue;
                    }
                    if (count++ > 0) {
                        strBuffer.append(",");
                    }
                    totalCfgNumPartCount += entity.getNumPartitions() * entity.getNumTopicStores();
                    strBuffer.append("{\"topicName\":\"").append(entity.getTopicName())
                            .append("\",\"topicStatusId\":").append(entity.getTopicStatusId())
                            .append(",\"brokerId\":").append(entity.getBrokerId())
                            .append(",\"brokerIp\":\"").append(entity.getBrokerIp())
                            .append("\",\"brokerPort\":").append(entity.getBrokerPort())
                            .append(",\"numPartitions\":").append(entity.getNumPartitions())
                            .append(",\"unflushThreshold\":").append(entity.getUnflushThreshold())
                            .append(",\"unflushInterval\":").append(entity.getUnflushInterval())
                            .append(",\"unFlushDataHold\":").append(entity.getunFlushDataHold())
                            .append(",\"deleteWhen\":\"").append(entity.getDeleteWhen())
                            .append("\",\"deletePolicy\":\"").append(entity.getDeletePolicy())
                            .append("\",\"acceptPublish\":").append(String.valueOf(entity.getAcceptPublish()))
                            .append(",\"acceptSubscribe\":").append(String.valueOf(entity.getAcceptSubscribe()))
                            .append(",\"numTopicStores\":").append(entity.getNumTopicStores())
                            .append(",\"memCacheMsgSizeInMB\":").append(entity.getMemCacheMsgSizeInMB())
                            .append(",\"memCacheFlushIntvl\":").append(entity.getMemCacheFlushIntvl())
                            .append(",\"memCacheMsgCntInK\":").append(entity.getMemCacheMsgCntInK())
                            .append(",\"createUser\":\"").append(entity.getCreateUser())
                            .append("\",\"createDate\":\"").append(formatter.format(entity.getCreateDate()))
                            .append("\",\"modifyUser\":\"").append(entity.getModifyUser())
                            .append("\",\"modifyDate\":\"").append(formatter.format(entity.getModifyDate()))
                            .append("\",\"runInfo\":{");
                    String strManageStatus = "-";
                    BdbBrokerConfEntity brokerConfEntity =
                            brokerConfManage.getBrokerDefaultConfigStoreInfo(entity.getBrokerId());
                    if (brokerConfEntity != null) {
                        int manageStatus = brokerConfEntity.getManageStatus();
                        strManageStatus = WebParameterUtils.getBrokerManageStatusStr(manageStatus);
                        if (manageStatus >= TStatusConstants.STATUS_MANAGE_ONLINE) {
                            if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE) {
                                isAcceptPublish = true;
                                isAcceptSubscribe = true;
                            } else if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE) {
                                isAcceptPublish = false;
                                isAcceptSubscribe = true;
                            } else if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ) {
                                isAcceptPublish = true;
                                isAcceptSubscribe = false;
                            }
                        }
                    }
                    BrokerInfo broker =
                            new BrokerInfo(entity.getBrokerId(), entity.getBrokerIp(), entity.getBrokerPort());
                    TopicInfo topicInfo = topicPSInfoManager.getTopicInfo(entity.getTopicName(), broker);
                    if (topicInfo == null) {
                        strBuffer.append("\"acceptPublish\":\"-\"").append(",\"acceptSubscribe\":\"-\"")
                                .append(",\"numPartitions\":\"-\"").append(",\"brokerManageStatus\":\"-\"");
                    } else {
                        if (isAcceptPublish) {
                            strBuffer.append("\"acceptPublish\":").append(topicInfo.isAcceptPublish());
                            if (topicInfo.isAcceptPublish()) {
                                isSrvAcceptPublish = true;
                            }
                        } else {
                            strBuffer.append("\"acceptPublish\":false");
                        }
                        if (isAcceptSubscribe) {
                            strBuffer.append(",\"acceptSubscribe\":").append(topicInfo.isAcceptSubscribe());
                            if (topicInfo.isAcceptSubscribe()) {
                                isSrvAcceptSubscribe = true;
                            }
                        } else {
                            strBuffer.append(",\"acceptSubscribe\":false");
                        }
                        totalRunNumPartCount += topicInfo.getPartitionNum() * topicInfo.getTopicStoreNum();
                        strBuffer.append(",\"numPartitions\":").append(topicInfo.getPartitionNum())
                                .append(",\"numTopicStores\":").append(topicInfo.getTopicStoreNum())
                                .append(",\"brokerManageStatus\":\"").append(strManageStatus).append("\"");
                    }
                    strBuffer.append("}}");
                }
                strBuffer.append("],\"infoCount\":").append(count)
                        .append(",\"totalCfgNumPart\":").append(totalCfgNumPartCount)
                        .append(",\"isSrvAcceptPublish\":").append(isSrvAcceptPublish)
                        .append(",\"isSrvAcceptSubscribe\":").append(isSrvAcceptSubscribe)
                        .append(",\"totalRunNumPartCount\":").append(totalRunNumPartCount)
                        .append(",\"authData\":{");
                BdbTopicAuthControlEntity authEntity =
                        brokerConfManage.getBdbEnableAuthControlByTopicName(entry.getKey());
                if (authEntity != null) {
                    strBuffer.append("\"enableAuthControl\":").append(authEntity.isEnableAuthControl())
                            .append(",\"createUser\":\"").append(authEntity.getCreateUser())
                            .append("\",\"createDate\":\"").append(formatter.format(authEntity.getCreateDate()))
                            .append("\",\"authConsumeGroup\":[");
                    List<BdbConsumerGroupEntity> webConsumerGroupEntities =
                            brokerConfManage.getBdbAllowedConsumerGroups(entry.getKey());
                    int countJ = 0;
                    for (BdbConsumerGroupEntity groupEntity : webConsumerGroupEntities) {
                        if (countJ++ > 0) {
                            strBuffer.append(",");
                        }
                        strBuffer.append("{\"groupName\":\"").append(groupEntity.getConsumerGroupName())
                                .append("\",\"createUser\":\"").append(groupEntity.getRecordCreateUser())
                                .append("\",\"createDate\":\"")
                                .append(formatter.format(groupEntity.getRecordCreateDate()))
                                .append("\"}");
                    }
                    strBuffer.append("],\"groupCount\":").append(countJ).append(",\"authFilterCondSet\":[");
                    List<BdbGroupFilterCondEntity> filterConds =
                            brokerConfManage.getBdbAllowedGroupFilterConds(entry.getKey());
                    int countY = 0;
                    for (BdbGroupFilterCondEntity itemCond : filterConds) {
                        if (countY++ > 0) {
                            strBuffer.append(",");
                        }
                        strBuffer.append("{\"groupName\":\"").append(itemCond.getConsumerGroupName())
                                .append("\",\"condStatus\":").append(itemCond.getControlStatus());
                        if (itemCond.getAttributes().length() <= 2) {
                            strBuffer.append(",\"filterConds\":\"\"");
                        } else {
                            strBuffer.append(",\"filterConds\":\"").append(itemCond.getAttributes()).append("\"");
                        }
                        strBuffer.append(",\"createUser\":\"").append(itemCond.getCreateUser())
                                .append("\",\"createDate\":\"").append(formatter.format(itemCond.getCreateDate()))
                                .append("\"}");
                    }
                    strBuffer.append("],\"filterCount\":").append(countY);
                }
                strBuffer.append("}}");
            }
            strBuffer.append("],\"dataCount\":").append(totalCount).append("}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\",\"dataCount\":0,\"data\":[]}");
        }
        return strBuffer;
    }

    /**
     * Delete topic info
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminDeleteTopicEntityInfo(HttpServletRequest req) throws Exception {
        return innModifyTopicStatusEntityInfo(req, TStatusConstants.STATUS_TOPIC_SOFT_DELETE);
    }

    /**
     * Remove topic info
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminRemoveTopicEntityInfo(HttpServletRequest req) throws Exception {
        return innModifyTopicStatusEntityInfo(req, TStatusConstants.STATUS_TOPIC_SOFT_REMOVE);
    }

    /**
     * Internal method to perform deletion and removal of topic
     *
     * @param req
     * @param topicStatusId
     * @return
     * @throws Exception
     */
    // #lizard forgives
    // TODO: shoud be private?
    public StringBuilder innModifyTopicStatusEntityInfo(HttpServletRequest req,
                                                        int topicStatusId) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            // Check if the request is authorized
            // and the parameters are valid
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            Set<String> bathRmvTopicNames =
                    WebParameterUtils.getBatchTopicNames(req.getParameter("topicName"),
                            true, false, null, strBuffer);
            Set<BdbBrokerConfEntity> bathInputTopicEntitySet =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"),
                            brokerConfManage, true, strBuffer);
            Set<Integer> changedBrokerSet = new HashSet<Integer>();
            Set<BdbTopicConfEntity> bathRmvBdbTopicEntitySet =
                    new HashSet<BdbTopicConfEntity>();

            // For the broker to perform, check its status
            // and check the config of the topic to see if the action could be performed
            for (BdbBrokerConfEntity brokerConfEntity : bathInputTopicEntitySet) {
                if (brokerConfEntity == null) {  // skip brokers whose config is not set
                    continue;
                }
                if (WebParameterUtils.checkBrokerInProcessing(brokerConfEntity.getBrokerId(), brokerConfManage,
                        strBuffer)) {  // skip brokers which is busy processing events
                    throw new Exception(strBuffer.toString());
                }
                if (WebParameterUtils.checkBrokerUnLoad(brokerConfEntity.getBrokerId(), brokerConfManage, strBuffer)) {
                    // skip brokers whose config is not loaded
                    throw new Exception(strBuffer.toString());
                }
                ConcurrentHashMap<String /* topic name */, BdbTopicConfEntity> brokerTopicEntityMap =
                        brokerConfManage.getBrokerTopicConfEntitySet(brokerConfEntity.getBrokerId());
                if ((brokerTopicEntityMap == null)
                        || (brokerTopicEntityMap.isEmpty())) {  // no topic configured on the broker
                    throw new Exception(strBuffer.append("No topic configure in broker=")
                            .append(brokerConfEntity.getBrokerId())
                            .append(", please confirm the configure first!").toString());
                }
                for (String itemTopicName : bathRmvTopicNames) {  // for each topic to remove
                    BdbTopicConfEntity bdbTopicConfEntity =
                            brokerTopicEntityMap.get(itemTopicName);
                    if (bdbTopicConfEntity == null) {  // topic entity does not exist on the broker
                        throw new Exception(strBuffer.append("Not found the topic ")
                                .append(itemTopicName)
                                .append("'s configure in broker=")
                                .append(brokerConfEntity.getBrokerId())
                                .append(", please confirm the configure first!").toString());
                    }
                    if (bdbTopicConfEntity.getAcceptPublish()
                            || bdbTopicConfEntity.getAcceptSubscribe()) {  // still accept publish and subscribe
                        throw new Exception(strBuffer.append("The topic ").append(itemTopicName)
                                .append("'s acceptPublish and acceptSubscribe parameters must be false in broker=")
                                .append(brokerConfEntity.getBrokerId())
                                .append(" before topic deleted!").toString());
                    }
                    if (topicStatusId == TStatusConstants.STATUS_TOPIC_SOFT_DELETE) {
                        if (!bdbTopicConfEntity.isValidTopicStatus()) {  // is soft delete
                            continue;
                        }
                    } else if (topicStatusId == TStatusConstants.STATUS_TOPIC_SOFT_REMOVE) {
                        if (bdbTopicConfEntity.getTopicStatusId() != TStatusConstants.STATUS_TOPIC_SOFT_DELETE) {
                            continue;
                        }
                    }
                    BdbTopicConfEntity queryEntity = new BdbTopicConfEntity();
                    queryEntity.setBrokerAndTopicInfo(brokerConfEntity.getBrokerId(),
                            brokerConfEntity.getBrokerIp(),
                            brokerConfEntity.getBrokerPort(),
                            itemTopicName);
                    bathRmvBdbTopicEntitySet.add(queryEntity);
                }
            }

            // Perform the action and check again
            try {
                for (BdbTopicConfEntity itemTopicEntity : bathRmvBdbTopicEntitySet) {
                    BdbBrokerConfEntity brokerConfEntity =
                            brokerConfManage.getBrokerDefaultConfigStoreInfo(itemTopicEntity.getBrokerId());
                    if (brokerConfEntity == null) {  // skip those brokers whose config is not set
                        continue;
                    }
                    ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicEntityMap =
                            brokerConfManage.getBrokerTopicConfEntitySet(brokerConfEntity.getBrokerId());
                    if ((brokerTopicEntityMap == null)
                            || (brokerTopicEntityMap.isEmpty())) {  // no topic configured on the broker
                        continue;
                    }
                    BdbTopicConfEntity bdbTopicConfEntity =
                            brokerTopicEntityMap.get(itemTopicEntity.getTopicName());
                    if (bdbTopicConfEntity == null) {
                        continue;
                    }
                    if (bdbTopicConfEntity.getAcceptPublish()
                            && bdbTopicConfEntity.getAcceptSubscribe()) {  // accept publish AND subscribe ??
                        continue;
                    }
                    if (topicStatusId == TStatusConstants.STATUS_TOPIC_SOFT_DELETE) {
                        if (!bdbTopicConfEntity.isValidTopicStatus()) {
                            continue;
                        }
                    } else if (topicStatusId == TStatusConstants.STATUS_TOPIC_SOFT_REMOVE) {
                        if (bdbTopicConfEntity.getTopicStatusId() != TStatusConstants.STATUS_TOPIC_SOFT_DELETE) {
                            continue;
                        }
                    }
                    if (WebParameterUtils.checkBrokerInProcessing(brokerConfEntity.getBrokerId(), brokerConfManage,
                            null)) {  // broker is busy processing event
                        continue;
                    }
                    if (WebParameterUtils.checkBrokerUnLoad(brokerConfEntity.getBrokerId(), brokerConfManage, null)) {
                        if (!changedBrokerSet.contains(brokerConfEntity.getBrokerId())) {  // already changed
                            continue;
                        }
                    }
                    inRmvTopicAuthControlInfo(itemTopicEntity.getTopicName(), modifyUser);
                    bdbTopicConfEntity.setTopicStatusId(topicStatusId);
                    boolean result = brokerConfManage.confModTopicConfig(bdbTopicConfEntity);
                    if (result) {
                        if (!brokerConfEntity.isConfDataUpdated()) {
                            brokerConfManage.updateBrokerConfChanged(brokerConfEntity.getBrokerId(), true, true);
                            changedBrokerSet.add(brokerConfEntity.getBrokerId());
                        }
                    }
                }
            } catch (Exception ee) {
                //
            }
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * Redo delete topic info
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminRedoDeleteTopicEntityInfo(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage, req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser", req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            Set<String> bathRmvTopicNames =
                    WebParameterUtils.getBatchTopicNames(req.getParameter("topicName"), true, false, null, strBuffer);
            Set<BdbBrokerConfEntity> bathBrokerEntitySet =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"), brokerConfManage, true,
                            strBuffer);
            List<BdbTopicConfEntity> bathRmvBdbTopicEntitys = new ArrayList<BdbTopicConfEntity>();
            for (BdbBrokerConfEntity brokerConfEntity : bathBrokerEntitySet) {
                if (brokerConfEntity == null) {
                    continue;
                }
                if (WebParameterUtils.checkBrokerInProcessing(brokerConfEntity.getBrokerId(), brokerConfManage,
                        strBuffer)) {
                    throw new Exception(strBuffer.toString());
                }
                if (WebParameterUtils.checkBrokerUnLoad(brokerConfEntity.getBrokerId(), brokerConfManage, strBuffer)) {
                    throw new Exception(strBuffer.toString());
                }
                ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicEntityMap =
                        brokerConfManage.getBrokerTopicConfEntitySet(brokerConfEntity.getBrokerId());
                if ((brokerTopicEntityMap == null) || (brokerTopicEntityMap.isEmpty())) {
                    throw new Exception(strBuffer.append("No topic configure in broker=")
                            .append(brokerConfEntity.getBrokerId())
                            .append(", please confirm the configure first!").toString());
                }
                for (String itemTopicName : bathRmvTopicNames) {
                    BdbTopicConfEntity bdbTopicConfEntity = brokerTopicEntityMap.get(itemTopicName);
                    if (bdbTopicConfEntity == null) {
                        throw new Exception(strBuffer.append("Not found the topic ").append(itemTopicName)
                                .append("'s configure in broker=").append(brokerConfEntity.getBrokerId())
                                .append(", please confirm the configure first!").toString());
                    }
                    if (bdbTopicConfEntity.getAcceptPublish() || bdbTopicConfEntity.getAcceptSubscribe()) {
                        throw new Exception(strBuffer.append("The topic ").append(itemTopicName)
                                .append("'s acceptPublish and acceptSubscribe parameters must be false in broker=")
                                .append(brokerConfEntity.getBrokerId()).append(" before topic deleted!").toString());
                    }
                    if (bdbTopicConfEntity.getTopicStatusId() != TStatusConstants.STATUS_TOPIC_SOFT_DELETE) {
                        if (bdbTopicConfEntity.isValidTopicStatus()) {
                            continue;
                        } else {
                            throw new Exception(strBuffer.append("Topic of ").append(itemTopicName)
                                    .append("is in removing flow in brokerId=").append(brokerConfEntity.getBrokerId())
                                    .append(", please wait until remove process finished!").toString());
                        }
                    }
                    BdbTopicConfEntity queryEntity = new BdbTopicConfEntity();
                    queryEntity.setBrokerAndTopicInfo(brokerConfEntity.getBrokerId(),
                            brokerConfEntity.getBrokerIp(), brokerConfEntity.getBrokerPort(), itemTopicName);
                    bathRmvBdbTopicEntitys.add(queryEntity);
                }
            }
            try {
                Set<Integer> changedBrokerSet = new HashSet<Integer>();
                for (BdbTopicConfEntity itemTopicEntity : bathRmvBdbTopicEntitys) {
                    BdbBrokerConfEntity brokerConfEntity =
                            brokerConfManage.getBrokerDefaultConfigStoreInfo(itemTopicEntity.getBrokerId());
                    if (brokerConfEntity == null) {
                        continue;
                    }
                    ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicEntityMap =
                            brokerConfManage.getBrokerTopicConfEntitySet(brokerConfEntity.getBrokerId());
                    if ((brokerTopicEntityMap == null) || (brokerTopicEntityMap.isEmpty())) {
                        continue;
                    }
                    BdbTopicConfEntity bdbTopicConfEntity = brokerTopicEntityMap.get(itemTopicEntity.getTopicName());
                    if ((bdbTopicConfEntity == null)
                            || (bdbTopicConfEntity.getAcceptPublish() && bdbTopicConfEntity.getAcceptSubscribe())
                            || (bdbTopicConfEntity.getTopicStatusId() != TStatusConstants.STATUS_TOPIC_SOFT_DELETE)
                            || (WebParameterUtils
                            .checkBrokerInProcessing(brokerConfEntity.getBrokerId(), brokerConfManage, null))) {
                        continue;
                    }
                    if (WebParameterUtils.checkBrokerUnLoad(brokerConfEntity.getBrokerId(), brokerConfManage, null)) {
                        if (!changedBrokerSet.contains(brokerConfEntity.getBrokerId())) {
                            continue;
                        }
                    }
                    inRmvTopicAuthControlInfo(itemTopicEntity.getTopicName(), modifyUser);
                    bdbTopicConfEntity.setTopicStatusId(TStatusConstants.STATUS_TOPIC_OK);
                    boolean result = brokerConfManage.confModTopicConfig(bdbTopicConfEntity);
                    BdbTopicAuthControlEntity tmpTopicAuthControl =
                            brokerConfManage.getBdbEnableAuthControlByTopicName(bdbTopicConfEntity.getTopicName());
                    if (tmpTopicAuthControl == null) {
                        brokerConfManage.confSetBdbTopicAuthControl(
                                new BdbTopicAuthControlEntity(bdbTopicConfEntity
                                        .getTopicName(), false, modifyUser, new Date()));
                    }
                    if (result) {
                        if (!brokerConfEntity.isConfDataUpdated()) {
                            brokerConfManage.updateBrokerConfChanged(
                                    brokerConfEntity.getBrokerId(), true, true);
                            changedBrokerSet.add(brokerConfEntity.getBrokerId());
                        }
                    }
                }
            } catch (Exception ee) {
                //
            }
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * Private method to remove topic authorization control info
     *
     * @param topicName
     * @param modifyUser
     * @throws Exception
     */
    private void inRmvTopicAuthControlInfo(final String topicName, final String modifyUser) throws Exception {
        BdbTopicAuthControlEntity webTopicAuthControlEntity = new BdbTopicAuthControlEntity();
        webTopicAuthControlEntity.setTopicName(topicName);
        List<BdbTopicAuthControlEntity> webTopicAuthControlEntities =
                brokerConfManage.confGetBdbTopicAuthCtrlEntityList(webTopicAuthControlEntity);
        if (!webTopicAuthControlEntities.isEmpty()) {
            try {
                BdbGroupFilterCondEntity filterCondEntity =
                        new BdbGroupFilterCondEntity();
                filterCondEntity.setTopicName(topicName);
                List<BdbGroupFilterCondEntity> webFilterCondEntities =
                        brokerConfManage.confGetBdbAllowedGroupFilterCondSet(filterCondEntity);
                if (!webFilterCondEntities.isEmpty()) {
                    filterCondEntity.setCreateUser(modifyUser);
                    brokerConfManage.confDelBdbAllowedGroupFilterCondSet(filterCondEntity);
                }
                BdbConsumerGroupEntity groupEntity =
                        new BdbConsumerGroupEntity();
                groupEntity.setGroupTopicName(topicName);
                List<BdbConsumerGroupEntity> webConsumerGroupEntities =
                        brokerConfManage.confGetBdbAllowedConsumerGroupSet(groupEntity);
                if (!webConsumerGroupEntities.isEmpty()) {
                    groupEntity.setRecordCreateUser(modifyUser);
                    brokerConfManage.confDelBdbAllowedConsumerGroupSet(groupEntity);
                }
                BdbTopicAuthControlEntity authEntity =
                        new BdbTopicAuthControlEntity();
                authEntity.setTopicName(topicName);
                authEntity.setCreateUser(modifyUser);
                brokerConfManage.confDeleteBdbTopicAuthControl(authEntity);
            } catch (Exception e) {
                logger.warn("Fun.inRmvTopicAuthControlInfo throw exception", e);
            }
        }
    }

    /**
     * Query broker topic config info
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminQueryBrokerTopicCfgAndRunInfo(
            HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        BdbTopicConfEntity webTopicEntity = new BdbTopicConfEntity();
        try {
            boolean hasCond = false;
            Set<String> bathOpTopicNames =
                    WebParameterUtils.getBatchTopicNames(req.getParameter("topicName"),
                            false, false, null, strBuffer);
            if (!bathOpTopicNames.isEmpty()) {
                hasCond = true;
                if (bathOpTopicNames.size() == 1) {
                    for (String topicName : bathOpTopicNames) {
                        webTopicEntity.setTopicName(topicName);
                    }
                }
            }
            webTopicEntity.setTopicStatusId(TBaseConstants.META_VALUE_UNDEFINED);
            webTopicEntity.setNumTopicStores(TBaseConstants.META_VALUE_UNDEFINED);
            webTopicEntity.setMemCacheMsgSizeInMB(TBaseConstants.META_VALUE_UNDEFINED);
            webTopicEntity.setMemCacheMsgCntInK(TBaseConstants.META_VALUE_UNDEFINED);
            webTopicEntity.setMemCacheFlushIntvl(TBaseConstants.META_VALUE_UNDEFINED);
            Map<Integer, BdbBrokerConfEntity> totalBrokers =
                    brokerConfManage.getBrokerConfStoreMap();
            Map<Integer, BrokerSyncStatusInfo> brokerSyncStatusInfoMap =
                    brokerConfManage.getBrokerRunSyncManageMap();
            Map<String, List<BdbTopicConfEntity>> topicQueryResults =
                    brokerConfManage.getBdbTopicEntityMap(webTopicEntity);
            List<Integer> brokerIds = new ArrayList<>();
            if (hasCond) {
                for (List<BdbTopicConfEntity> topicConfEntities : topicQueryResults.values()) {
                    if (topicConfEntities == null || topicConfEntities.isEmpty()) {
                        continue;
                    }
                    for (BdbTopicConfEntity topicConfEntity : topicConfEntities) {
                        if (topicConfEntity == null) {
                            continue;
                        }
                        brokerIds.add(topicConfEntity.getBrokerId());
                    }
                }

            } else {
                for (Integer brokerId : totalBrokers.keySet()) {
                    if (brokerId == null) {
                        continue;
                    }
                    brokerIds.add(brokerId);
                }
            }
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"data\":[");
            int totalCount = 0;
            for (Integer brokerId : brokerIds) {
                BdbBrokerConfEntity brokerEntity = totalBrokers.get(brokerId);
                if (brokerEntity == null) {
                    continue;
                }
                if (totalCount++ > 0) {
                    strBuffer.append(",");
                }
                boolean isAcceptPublish = false;
                boolean isAcceptSubscribe = false;
                int totalNumPartCount = 0;
                int totalStoreNum = 0;
                strBuffer.append("{\"brokerId\":").append(brokerEntity.getBrokerId())
                        .append(",\"brokerIp\":\"").append(brokerEntity.getBrokerIp())
                        .append("\",\"brokerPort\":").append(brokerEntity.getBrokerPort())
                        .append(",\"runInfo\":{");
                String strManageStatus = "-";
                BdbBrokerConfEntity brokerConfEntity =
                        brokerConfManage.getBrokerDefaultConfigStoreInfo(brokerEntity.getBrokerId());
                if (brokerConfEntity != null) {
                    int manageStatus = brokerConfEntity.getManageStatus();
                    strManageStatus = WebParameterUtils.getBrokerManageStatusStr(manageStatus);
                    if (manageStatus >= TStatusConstants.STATUS_MANAGE_ONLINE) {
                        if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE) {
                            isAcceptPublish = true;
                            isAcceptSubscribe = true;
                        } else if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE) {
                            isAcceptPublish = false;
                            isAcceptSubscribe = true;
                        } else if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ) {
                            isAcceptPublish = true;
                            isAcceptSubscribe = false;
                        }
                    }
                }
                BrokerSyncStatusInfo brokerSyncStatusInfo =
                        brokerSyncStatusInfoMap.get(brokerEntity.getBrokerId());
                if (brokerSyncStatusInfo == null) {
                    strBuffer.append("\"acceptPublish\":\"-\"")
                            .append(",\"acceptSubscribe\":\"-\"")
                            .append(",\"totalPartitionNum\":\"-\"")
                            .append(",\"totalTopicStoreNum\":\"-\"")
                            .append(",\"brokerManageStatus\":\"-\"");
                } else {
                    if (isAcceptPublish) {
                        strBuffer.append("\"acceptPublish\":")
                                .append(brokerSyncStatusInfo.isAcceptPublish());
                    } else {
                        strBuffer.append("\"acceptPublish\":false");
                    }
                    if (isAcceptSubscribe) {
                        strBuffer.append(",\"acceptSubscribe\":")
                                .append(brokerSyncStatusInfo.isAcceptSubscribe());
                    } else {
                        strBuffer.append(",\"acceptSubscribe\":false");
                    }
                    for (List<BdbTopicConfEntity> topicEntityList : topicQueryResults.values()) {
                        if (topicEntityList == null || topicEntityList.isEmpty()) {
                            continue;
                        }
                        for (BdbTopicConfEntity topicEntity : topicEntityList) {
                            if (topicEntity == null) {
                                continue;
                            }
                            totalStoreNum += topicEntity.getNumTopicStores();
                            totalNumPartCount +=
                                    topicEntity.getNumTopicStores() * topicEntity.getNumPartitions();
                        }
                    }
                    strBuffer.append(",\"totalPartitionNum\":")
                            .append(totalNumPartCount)
                            .append(",\"totalTopicStoreNum\":")
                            .append(totalStoreNum)
                            .append(",\"brokerManageStatus\":\"")
                            .append(strManageStatus).append("\"");
                }
                strBuffer.append("}}");
            }
            strBuffer.append("],\"dataCount\":").append(totalCount).append("}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\",\"dataCount\":0,\"data\":[]}");
        }
        return strBuffer;
    }

    /**
     * Modify topic info
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminModifyTopicEntityInfo(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder();
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage, req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser", req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate", req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, new Date());
            Set<String> bathModTopicNames =
                    WebParameterUtils.getBatchTopicNames(req.getParameter("topicName"),
                            true, false, null, strBuffer);
            Set<BdbBrokerConfEntity> bathBrokerEntitySet =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"),
                            brokerConfManage, true, strBuffer);
            String deleteWhen =
                    WebParameterUtils.validDecodeStringParameter("deleteWhen", req.getParameter("deleteWhen"),
                            TServerConstants.CFG_DELETEWHEN_MAX_LENGTH, false, null);
            String deletePolicy =
                    WebParameterUtils.validDecodeStringParameter("deletePolicy", req.getParameter("deletePolicy"),
                            TServerConstants.CFG_DELETEPOLICY_MAX_LENGTH, false, null);
            int numPartitions =
                    WebParameterUtils.validIntDataParameter("numPartitions",
                            req.getParameter("numPartitions"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
            int unflushThreshold =
                    WebParameterUtils.validIntDataParameter("unflushThreshold",
                            req.getParameter("unflushThreshold"), false, TBaseConstants.META_VALUE_UNDEFINED, 0);
            int unflushInterval =
                    WebParameterUtils.validIntDataParameter("unflushInterval",
                            req.getParameter("unflushInterval"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
            int numTopicStores =
                    WebParameterUtils.validIntDataParameter("numTopicStores",
                            req.getParameter("numTopicStores"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
            int memCacheMsgCntInK =
                    WebParameterUtils.validIntDataParameter("memCacheMsgCntInK",
                            req.getParameter("memCacheMsgCntInK"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
            int memCacheMsgSizeInMB =
                    WebParameterUtils.validIntDataParameter("memCacheMsgSizeInMB",
                            req.getParameter("memCacheMsgSizeInMB"), false, TBaseConstants.META_VALUE_UNDEFINED, 2);
            memCacheMsgSizeInMB = memCacheMsgSizeInMB >= 2048 ? 2048 : memCacheMsgSizeInMB;
            int memCacheFlushIntvl =
                    WebParameterUtils.validIntDataParameter("memCacheFlushIntvl",
                            req.getParameter("memCacheFlushIntvl"), false, TBaseConstants.META_VALUE_UNDEFINED, 4000);
            int unFlushDataHold = unflushThreshold;
            List<BdbTopicConfEntity> bathModBdbTopicEntitys = new ArrayList<BdbTopicConfEntity>();
            for (BdbBrokerConfEntity tgtEntity : bathBrokerEntitySet) {
                if (tgtEntity == null) {
                    continue;
                }
                if (WebParameterUtils.checkBrokerInProcessing(tgtEntity.getBrokerId(), brokerConfManage, strBuffer)) {
                    throw new Exception(strBuffer.toString());
                }
                ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicEntityMap =
                        brokerConfManage.getBrokerTopicConfEntitySet(tgtEntity.getBrokerId());
                if ((brokerTopicEntityMap == null) || (brokerTopicEntityMap.isEmpty())) {
                    throw new Exception(strBuffer.append("No topic configure in broker=")
                            .append(tgtEntity.getBrokerId()).append(", please confirm the configure first!")
                            .toString());
                }
                for (String itemTopicName : bathModTopicNames) {
                    BdbTopicConfEntity oldEntity = brokerTopicEntityMap.get(itemTopicName);
                    if (oldEntity == null) {
                        throw new Exception(strBuffer.append("Not found the topic ")
                                .append(itemTopicName).append("'s configure in broker=")
                                .append(tgtEntity.getBrokerId()).append(", please confirm the configure first!")
                                .toString());
                    }
                    if (!oldEntity.isValidTopicStatus()) {
                        throw new Exception(strBuffer.append("Topic of ").append(itemTopicName)
                                .append("is deleted softly in brokerId=").append(tgtEntity.getBrokerId())
                                .append(", please resume the record or hard removed first!").toString());
                    }
                    boolean foundChange = false;
                    BdbTopicConfEntity newEntity =
                            new BdbTopicConfEntity(oldEntity.getBrokerId(), oldEntity.getBrokerIp(),
                                    oldEntity.getBrokerPort(), oldEntity.getTopicName(),
                                    oldEntity.getNumPartitions(), oldEntity.getUnflushThreshold(),
                                    oldEntity.getUnflushInterval(), oldEntity.getDeleteWhen(),
                                    oldEntity.getDeletePolicy(), oldEntity.getAcceptPublish(),
                                    oldEntity.getAcceptSubscribe(), oldEntity.getNumTopicStores(),
                                    oldEntity.getAttributes(),
                                    oldEntity.getCreateUser(), oldEntity.getCreateDate(), modifyUser, modifyDate);
                    if ((!TStringUtils.isBlank(deleteWhen)) && (!deleteWhen.equals(oldEntity.getDeleteWhen()))) {
                        foundChange = true;
                        newEntity.setDeleteWhen(deleteWhen);
                    }
                    if ((!TStringUtils.isBlank(deletePolicy)) && (!deletePolicy.equals(oldEntity.getDeletePolicy()))) {
                        foundChange = true;
                        newEntity.setDeletePolicy(deletePolicy);
                    }
                    if ((numPartitions > 0) && (numPartitions != oldEntity.getNumPartitions())) {
                        if (numPartitions < oldEntity.getNumPartitions()) {
                            throw new Exception(strBuffer
                                    .append("Partition value is less than before," +
                                            "please confirm the configure first! brokerId=")
                                    .append(oldEntity.getBrokerId())
                                    .append(", topicName=").append(oldEntity.getTopicName())
                                    .append(", old Partition value is ").append(oldEntity.getNumPartitions())
                                    .append(", new Partition value is ").append(numPartitions).toString());
                        }
                        foundChange = true;
                        newEntity.setNumPartitions(numPartitions);
                    }
                    if ((unflushThreshold >= 0) && (unflushThreshold != oldEntity.getUnflushThreshold())) {
                        foundChange = true;
                        newEntity.setUnflushThreshold(unflushThreshold);
                    }
                    if (unFlushDataHold >= 0 && unFlushDataHold != oldEntity.getunFlushDataHold()) {
                        foundChange = true;
                        newEntity.setunFlushDataHold(unFlushDataHold);
                    }
                    if (memCacheMsgCntInK >= 0 && memCacheMsgCntInK != oldEntity.getMemCacheMsgCntInK()) {
                        foundChange = true;
                        newEntity.appendAttributes(TokenConstants.TOKEN_MCACHE_MSG_CNT,
                                String.valueOf(memCacheMsgCntInK));
                    }
                    if (memCacheMsgSizeInMB >= 0 && memCacheMsgSizeInMB != oldEntity.getMemCacheMsgSizeInMB()) {
                        foundChange = true;
                        newEntity.appendAttributes(TokenConstants.TOKEN_MCACHE_MSG_SIZE,
                                String.valueOf(memCacheMsgSizeInMB));
                    }
                    if (memCacheFlushIntvl >= 0 && memCacheFlushIntvl != oldEntity.getMemCacheFlushIntvl()) {
                        foundChange = true;
                        newEntity.appendAttributes(TokenConstants.TOKEN_MCACHE_FLUSH_INTVL,
                                String.valueOf(memCacheFlushIntvl));
                    }
                    if ((numTopicStores > 0) && (numTopicStores != oldEntity.getNumTopicStores())) {
                        if (numTopicStores < oldEntity.getNumTopicStores()) {
                            throw new Exception(strBuffer
                                    .append("TopicStores value is less than before," +
                                            "please confirm the configure first! brokerId=")
                                    .append(oldEntity.getBrokerId())
                                    .append(", topicName=").append(oldEntity.getTopicName())
                                    .append(", old TopicStores value is ").append(oldEntity.getNumTopicStores())
                                    .append(", new TopicStores value is ").append(numTopicStores).toString());
                        }
                        foundChange = true;
                        newEntity.setNumTopicStores(numTopicStores);
                    }
                    if ((unflushInterval > 0) && (unflushInterval != oldEntity.getUnflushInterval())) {
                        foundChange = true;
                        newEntity.setUnflushInterval(unflushInterval);
                    }
                    String publishParaStr = req.getParameter("acceptPublish");
                    if (!TStringUtils.isBlank(publishParaStr)) {
                        boolean acceptPublish =
                                WebParameterUtils.validBooleanDataParameter("acceptPublish",
                                        req.getParameter("acceptPublish"), true, true);
                        if (acceptPublish != oldEntity.getAcceptPublish()) {
                            foundChange = true;
                            newEntity.setAcceptPublish(acceptPublish);
                        }
                    }
                    String subscribeParaStr = req.getParameter("acceptSubscribe");
                    if (!TStringUtils.isBlank(subscribeParaStr)) {
                        boolean acceptSubscribe =
                                WebParameterUtils.validBooleanDataParameter("acceptSubscribe",
                                        req.getParameter("acceptSubscribe"), true, true);
                        if (acceptSubscribe != oldEntity.getAcceptSubscribe()) {
                            foundChange = true;
                            newEntity.setAcceptSubscribe(acceptSubscribe);
                        }
                    }
                    if (!foundChange) {
                        continue;
                    }
                    bathModBdbTopicEntitys.add(newEntity);
                }
            }
            if (bathModBdbTopicEntitys.isEmpty()) {
                throw new Exception("Not found data changed, please confirm the topic configure!");
            }
            try {
                for (BdbTopicConfEntity itemTopicEntity : bathModBdbTopicEntitys) {
                    BdbBrokerConfEntity brokerConfEntity =
                            brokerConfManage.getBrokerDefaultConfigStoreInfo(itemTopicEntity.getBrokerId());
                    if (brokerConfEntity == null) {
                        continue;
                    }
                    ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicEntityMap =
                            brokerConfManage.getBrokerTopicConfEntitySet(brokerConfEntity.getBrokerId());
                    if ((brokerTopicEntityMap == null)
                            || (brokerTopicEntityMap.isEmpty())) {
                        continue;
                    }
                    BdbTopicConfEntity oldEntity =
                            brokerTopicEntityMap.get(itemTopicEntity.getTopicName());
                    if (oldEntity == null) {
                        continue;
                    }
                    boolean isFastStart = true;
                    if ((itemTopicEntity.getNumPartitions() != oldEntity.getNumPartitions())
                            || (itemTopicEntity.getNumTopicStores() != oldEntity.getNumTopicStores())
                            || (itemTopicEntity.getAcceptSubscribe() != oldEntity.getAcceptSubscribe())) {
                        isFastStart = false;
                    }
                    if (WebParameterUtils.checkBrokerInProcessing(itemTopicEntity.getBrokerId(), brokerConfManage,
                            null)) {
                        continue;
                    }
                    BrokerSyncStatusInfo brokerSyncStatusInfo =
                            brokerConfManage.getBrokerRunSyncStatusInfo(itemTopicEntity.getBrokerId());
                    boolean result = brokerConfManage.confModTopicConfig(itemTopicEntity);
                    if (result) {
                        if ((brokerSyncStatusInfo != null) && !isFastStart) {
                            brokerSyncStatusInfo.setFastStart(isFastStart);
                        }
                        brokerConfEntity =
                                brokerConfManage.getBrokerDefaultConfigStoreInfo(itemTopicEntity.getBrokerId());
                        if (brokerConfEntity != null && !brokerConfEntity.isConfDataUpdated()) {
                            brokerConfManage.updateBrokerConfChanged(
                                    brokerConfEntity.getBrokerId(), true, isFastStart);
                        }
                    }
                }
            } catch (Exception ee) {
                //
            }
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }
}
