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

import static java.lang.Math.abs;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.common.TServerConstants;
import com.tencent.tubemq.server.common.TStatusConstants;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerInfoHolder;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerSyncStatusInfo;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker的缺省配置操作类,包括新增broker配置记录,变更配置,删除配置,以及修改broker的线上管理状态
 * 需要注意的是每个IP只能部署一个Broker,每个brokerId必须要唯一
 * 相关逻辑比较简单,API代码里有对应要求,代码只是做对应的处理
 * <p>
 * The class to handle the default config of broker, including:
 * - Add config
 * - Update config
 * - Delete config
 * And manage the broker status.
 * <p>
 * Please note that one IP could only host one broker, and brokerId must be unique
 */
public class WebBrokerDefConfHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(WebBrokerDefConfHandler.class);
    private TMaster master;
    private BrokerConfManage brokerConfManage;

    /**
     * Constructor
     *
     * @param master tube master
     */
    public WebBrokerDefConfHandler(TMaster master) {
        this.master = master;
        this.brokerConfManage = this.master.getMasterTopicManage();
    }

    // #lizard forgives

    /**
     * Fast start a broker?
     *
     * @param webMaster
     * @param brokeId
     * @param oldManagStatus
     * @param newManageStatus
     * @return
     */
    public static boolean isBrokerStartNeedFast(BrokerConfManage webMaster,
                                                int brokeId,
                                                int oldManagStatus,
                                                int newManageStatus) {
        ConcurrentHashMap<String, BdbTopicConfEntity> bdbTopicConfEntMap =
                webMaster.getBrokerTopicConfEntitySet(brokeId);
        if ((bdbTopicConfEntMap == null)
                || (bdbTopicConfEntMap.isEmpty())) {
            return true;
        }
        BrokerSyncStatusInfo brokerSyncStatusInfo =
                webMaster.getBrokerRunSyncStatusInfo(brokeId);
        if ((brokerSyncStatusInfo == null)
                || (!brokerSyncStatusInfo.isBrokerRegister())) {
            return true;
        }
        boolean isNeedFastStart =
                brokerSyncStatusInfo.isFastStart();
        if (isNeedFastStart) {
            switch (newManageStatus) {
                case TStatusConstants.STATUS_MANAGE_ONLINE: {
                    if ((oldManagStatus == TStatusConstants.STATUS_MANAGE_APPLY)
                            || (oldManagStatus == TStatusConstants.STATUS_MANAGE_OFFLINE)
                            || (oldManagStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ)) {
                        isNeedFastStart = false;
                    }
                    if (oldManagStatus == TStatusConstants.STATUS_MANAGE_ONLINE) {
                        if ((brokerSyncStatusInfo.isBrokerConfChaned())
                                || (!brokerSyncStatusInfo.isBrokerLoaded())) {
                            isNeedFastStart = false;
                        }
                    }
                }
                break;
                case TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE: {
                    if ((oldManagStatus == TStatusConstants.STATUS_MANAGE_APPLY)
                            || (oldManagStatus == TStatusConstants.STATUS_MANAGE_OFFLINE)) {
                        isNeedFastStart = false;
                    }
                    if (oldManagStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE) {
                        if ((brokerSyncStatusInfo.isBrokerConfChaned())
                                || (!brokerSyncStatusInfo.isBrokerLoaded())) {
                            isNeedFastStart = false;
                        }
                    }
                }
                break;
                case TStatusConstants.STATUS_MANAGE_OFFLINE: {
                    if ((oldManagStatus == TStatusConstants.STATUS_MANAGE_ONLINE)
                            || (oldManagStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE)) {
                        isNeedFastStart = false;
                    }
                }
                break;
                default: {
                    //
                }
            }
        }
        return isNeedFastStart;
    }

    /**
     * Add default config to a broker
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminAddBrokerDefConfEntityInfo(
            HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String brokerIp =
                    WebParameterUtils.validAddressParameter("brokerIp",
                            req.getParameter("brokerIp"),
                            TBaseConstants.META_MAX_BROKER_IP_LENGTH,
                            true, "");
            int brokerPort =
                    WebParameterUtils.validIntDataParameter("brokerPort",
                            req.getParameter("brokerPort"),
                            false, 8123, 1);
            int brokerId =
                    WebParameterUtils.validIntDataParameter("brokerId",
                            req.getParameter("brokerId"),
                            false, 0, 0);
            if (brokerId <= 0) {
                try {
                    brokerId = abs(AddressUtils.ipToInt(brokerIp));
                } catch (Exception e) {
                    throw new Exception(strBuffer
                            .append("Get brokerId by brokerIp error !, exception is :")
                            .append(e.toString()).toString());
                }
            }
            BdbBrokerConfEntity oldEntity =
                    brokerConfManage.getBrokerDefaultConfigStoreInfo(brokerId);
            if (oldEntity != null) {
                throw new Exception(strBuffer
                        .append("Duplicated broker default configure record by (brokerId or brokerIp), " +
                                "query index is :")
                        .append("brokerId=").append(brokerId)
                        .append(",brokerIp=").append(brokerIp).toString());
            }
            ConcurrentHashMap<Integer, BdbBrokerConfEntity> bdbBrokerConfEntityMap =
                    brokerConfManage.getBrokerConfStoreMap();
            for (BdbBrokerConfEntity brokerConfEntity : bdbBrokerConfEntityMap.values()) {
                if (brokerConfEntity.getBrokerIp().equals(brokerIp)
                        && brokerConfEntity.getBrokerPort() == brokerPort) {
                    strBuffer.append("Duplicated broker default configure record by (brokerIp and brokerPort), " +
                            "query index is :")
                            .append("brokerIp=").append(brokerIp)
                            .append(",brokerPort=").append(brokerPort)
                            .append(",existed record is ").append(brokerConfEntity);
                    throw new Exception(strBuffer.toString());
                }
            }
            String createUser =
                    WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            String deleteWhen =
                    WebParameterUtils.validDecodeStringParameter("deleteWhen",
                            req.getParameter("deleteWhen"),
                            TServerConstants.CFG_DELETEWHEN_MAX_LENGTH, false,
                            "0 0 6,18 * * ?");
            String deletePolicy =
                    WebParameterUtils.validDecodeStringParameter("deletePolicy",
                            req.getParameter("deletePolicy"),
                            TServerConstants.CFG_DELETEPOLICY_MAX_LENGTH, false,
                            "delete,168h");
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            false, createUser);
            Date createDate =
                    WebParameterUtils.validDateParameter("createDate",
                            req.getParameter("createDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, new Date());
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate",
                            req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, createDate);
            int numPartitions =
                    WebParameterUtils.validIntDataParameter("numPartitions",
                            req.getParameter("numPartitions"),
                            false, 1, 1);
            int unflushThreshold =
                    WebParameterUtils.validIntDataParameter("unflushThreshold",
                            req.getParameter("unflushThreshold"),
                            false, 1000, 0);
            int unflushInterval =
                    WebParameterUtils.validIntDataParameter("unflushInterval",
                            req.getParameter("unflushInterval"),
                            false, 10000, 1);
            int memCacheMsgCntInK =
                    WebParameterUtils.validIntDataParameter("memCacheMsgCntInK",
                            req.getParameter("memCacheMsgCntInK"),
                            false, 10, 1);
            int memCacheMsgSizeInMB =
                    WebParameterUtils.validIntDataParameter("memCacheMsgSizeInMB",
                            req.getParameter("memCacheMsgSizeInMB"),
                            false, 2, 2);
            memCacheMsgSizeInMB = memCacheMsgSizeInMB >= 2048 ? 2048 : memCacheMsgSizeInMB;
            int memCacheFlushIntvl =
                    WebParameterUtils.validIntDataParameter("memCacheFlushIntvl",
                            req.getParameter("memCacheFlushIntvl"),
                            false, 20000, 4000);
            boolean acceptPublish =
                    WebParameterUtils.validBooleanDataParameter("acceptPublish",
                            req.getParameter("acceptPublish"),
                            false, true);
            boolean acceptSubscribe =
                    WebParameterUtils.validBooleanDataParameter("acceptSubscribe",
                            req.getParameter("acceptSubscribe"),
                            false, true);
            int manageStatus = TStatusConstants.STATUS_MANAGE_APPLY;
            int numTopicStores =
                    WebParameterUtils.validIntDataParameter("numTopicStores",
                            req.getParameter("numTopicStores"), false, 1, 1);
            int unFlushDataHold = unflushThreshold;
            int brokerTlsPort =
                    WebParameterUtils.validIntDataParameter("brokerTLSPort",
                            req.getParameter("brokerTLSPort"), false,
                            TBaseConstants.META_DEFAULT_BROKER_TLS_PORT, 0);
            String attributes =
                    strBuffer.append(TokenConstants.TOKEN_STORE_NUM).append(TokenConstants.EQ).append(numTopicStores)
                            .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_DATA_UNFLUSHHOLD)
                            .append(TokenConstants.EQ).append(unFlushDataHold)
                            .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_MCACHE_MSG_CNT)
                            .append(TokenConstants.EQ).append(memCacheMsgCntInK)
                            .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_MCACHE_MSG_SIZE)
                            .append(TokenConstants.EQ).append(memCacheMsgSizeInMB).append(TokenConstants.SEGMENT_SEP)
                            .append(TokenConstants.TOKEN_MCACHE_FLUSH_INTVL)
                            .append(TokenConstants.EQ).append(memCacheFlushIntvl)
                            .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_TLS_PORT)
                            .append(TokenConstants.EQ).append(brokerTlsPort).toString();
            strBuffer.delete(0, strBuffer.length());
            BdbBrokerConfEntity brokerConfEntity =
                    new BdbBrokerConfEntity(brokerId, brokerIp, brokerPort,
                            numPartitions, unflushThreshold, unflushInterval,
                            deleteWhen, deletePolicy, manageStatus, acceptPublish,
                            acceptSubscribe, attributes, true, false, createUser,
                            createDate, modifyUser, modifyDate);
            brokerConfManage.confAddBrokerDefaultConfig(brokerConfEntity);
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * Add default config to brokers in batch
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminBathAddBrokerDefConfEntityInfo(HttpServletRequest req) throws Exception {
        // #lizard forgives
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
            List<Map<String, Object>> brokerJsonArray =
                    WebParameterUtils.checkAndGetJsonArray("brokerJsonSet",
                            req.getParameter("brokerJsonSet"), TBaseConstants.META_VALUE_UNDEFINED, true);
            if ((brokerJsonArray == null) || (brokerJsonArray.isEmpty())) {
                throw new Exception("Null value of brokerJsonSet, please set the value first!");
            }
            HashMap<String, BdbBrokerConfEntity> inBrokerConfEntiyMap = new HashMap<String, BdbBrokerConfEntity>();
            ConcurrentHashMap<Integer, BdbBrokerConfEntity> bdbBrokerConfEntityMap =
                    brokerConfManage.getBrokerConfStoreMap();
            for (int count = 0; count < brokerJsonArray.size(); count++) {
                Map<String, Object> jsonObject = brokerJsonArray.get(count);
                try {
                    String brokerIp =
                            WebParameterUtils.validAddressParameter("brokerIp", jsonObject.get("brokerIp"),
                                    TBaseConstants.META_MAX_BROKER_IP_LENGTH, false, "");
                    int brokerPort =
                            WebParameterUtils.validIntDataParameter("brokerPort",
                                    jsonObject.get("brokerPort"), false, 8123, 0);
                    int brokerId =
                            WebParameterUtils.validIntDataParameter("brokerId",
                                    jsonObject.get("brokerId"), false, 0, 0);
                    if (brokerId <= 0) {
                        brokerIp =
                                WebParameterUtils.validAddressParameter("brokerIp", jsonObject.get("brokerIp"),
                                        TBaseConstants.META_MAX_BROKER_IP_LENGTH, true, "");
                        try {
                            brokerId = abs(AddressUtils.ipToInt(brokerIp));
                        } catch (Exception e) {
                            throw new Exception(strBuffer
                                    .append("Get brokerId by brokerIp error !, record is : ")
                                    .append(jsonObject.toString()).append("exception is :").append(e.toString())
                                    .toString());
                        }
                    }
                    BdbBrokerConfEntity oldEntity =
                            bdbBrokerConfEntityMap.get(brokerId);
                    if (oldEntity != null) {
                        throw new Exception(strBuffer
                                .append("Duplicated broker default configure record by (brokerId or brokerIp), " +
                                        "query index is :")
                                .append("brokerId=").append(brokerId).append(",brokerIp=").append(brokerIp).toString());
                    }
                    for (BdbBrokerConfEntity brokerConfEntity : bdbBrokerConfEntityMap.values()) {
                        if (brokerConfEntity.getBrokerIp().equals(brokerIp)
                                && brokerConfEntity.getBrokerPort() == brokerPort) {
                            strBuffer.append(
                                    "Duplicate add broker default configure record by (brokerIp and brokerPort), " +
                                            "query index is :")
                                    .append("brokerIp=").append(brokerIp).append(",brokerPort=").append(brokerPort)
                                    .append(",existed record is ").append(brokerConfEntity);
                            throw new Exception(strBuffer.toString());
                        }
                    }
                    for (BdbBrokerConfEntity brokerConfEntity : inBrokerConfEntiyMap.values()) {
                        if (brokerConfEntity.getBrokerIp().equals(brokerIp)
                                && brokerConfEntity.getBrokerPort() == brokerPort) {
                            throw new Exception(strBuffer
                                    .append("Duplicate (brokerIp and brokerPort) in request records, " +
                                            "duplicated key is : ")
                                    .append("brokerIp=").append(brokerIp).append(",brokerPort=").append(brokerPort)
                                    .toString());
                        }
                        if (brokerConfEntity.getBrokerId() == brokerId) {
                            throw new Exception(strBuffer
                                    .append("Duplicate brokerId in request records, duplicated brokerId is : brokerId=")
                                    .append(brokerId).toString());
                        }
                    }
                    final String inputKey = strBuffer.append(brokerId).append("-")
                            .append(brokerIp).append("-").append(brokerPort).toString();
                    strBuffer.delete(0, strBuffer.length());
                    final String deleteWhen =
                            WebParameterUtils.validDecodeStringParameter("deleteWhen", jsonObject.get("deleteWhen"),
                                    TServerConstants.CFG_DELETEWHEN_MAX_LENGTH, false, "0 0 6,18 * * ?");
                    final String deletePolicy =
                            WebParameterUtils.validDecodeStringParameter("deletePolicy", jsonObject.get("deletePolicy"),
                                    TServerConstants.CFG_DELETEPOLICY_MAX_LENGTH, false, "delete,168h");
                    final int numPartitions =
                            WebParameterUtils.validIntDataParameter("numPartitions",
                                    jsonObject.get("numPartitions"), false, 1, 1);
                    final int unflushThreshold =
                            WebParameterUtils.validIntDataParameter("unflushThreshold",
                                    jsonObject.get("unflushThreshold"), false, 1000, 0);
                    final int unflushInterval =
                            WebParameterUtils.validIntDataParameter("unflushInterval",
                                    jsonObject.get("unflushInterval"), false, 10000, 1);
                    final boolean acceptPublish =
                            WebParameterUtils.validBooleanDataParameter("acceptPublish",
                                    jsonObject.get("acceptPublish"), false, true);
                    final boolean acceptSubscribe =
                            WebParameterUtils.validBooleanDataParameter("acceptSubscribe",
                                    jsonObject.get("acceptSubscribe"), false, true);
                    String itemCreateUser =
                            WebParameterUtils.validStringParameter("createUser", jsonObject.get("createUser"),
                                    TBaseConstants.META_MAX_USERNAME_LENGTH, false, null);
                    Date itemCreateDate =
                            WebParameterUtils.validDateParameter("createDate", jsonObject.get("createDate"),
                                    TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, null);
                    if (TStringUtils.isBlank(itemCreateUser) || itemCreateDate == null) {
                        itemCreateUser = createUser;
                        itemCreateDate = createDate;
                    }
                    int unFlushDataHold = unflushThreshold;
                    int brokerTlsPort =
                            WebParameterUtils.validIntDataParameter("brokerTLSPort",
                                    jsonObject.get("brokerTLSPort"), false,
                                    TBaseConstants.META_DEFAULT_BROKER_TLS_PORT, 0);
                    int manageStatus = TStatusConstants.STATUS_MANAGE_APPLY;
                    int numTopicStores =
                            WebParameterUtils.validIntDataParameter("numTopicStores",
                                    jsonObject.get("numTopicStores"), false, 1, 1);
                    int memCacheMsgCntInK =
                            WebParameterUtils.validIntDataParameter("memCacheMsgCntInK",
                                    jsonObject.get("memCacheMsgCntInK"), false, 10, 1);
                    int memCacheMsgSizeInMB =
                            WebParameterUtils.validIntDataParameter("memCacheMsgSizeInMB",
                                    jsonObject.get("memCacheMsgSizeInMB"), false, 2, 2);
                    memCacheMsgSizeInMB = memCacheMsgSizeInMB >= 2048 ? 2048 : memCacheMsgSizeInMB;
                    int memCacheFlushIntvl =
                            WebParameterUtils.validIntDataParameter("memCacheFlushIntvl",
                                    jsonObject.get("memCacheFlushIntvl"), false, 20000, 4000);
                    String attributes = strBuffer
                            .append(TokenConstants.TOKEN_STORE_NUM).append(TokenConstants.EQ).append(numTopicStores)
                            .append(TokenConstants.SEGMENT_SEP).append(TokenConstants.TOKEN_DATA_UNFLUSHHOLD)
                            .append(TokenConstants.EQ).append(unFlushDataHold).append(TokenConstants.SEGMENT_SEP)
                            .append(TokenConstants.TOKEN_MCACHE_MSG_CNT).append(TokenConstants.EQ)
                            .append(memCacheMsgCntInK).append(TokenConstants.SEGMENT_SEP)
                            .append(TokenConstants.TOKEN_MCACHE_MSG_SIZE).append(TokenConstants.EQ)
                            .append(memCacheMsgSizeInMB).append(TokenConstants.SEGMENT_SEP)
                            .append(TokenConstants.TOKEN_MCACHE_FLUSH_INTVL).append(TokenConstants.EQ)
                            .append(memCacheFlushIntvl).append(TokenConstants.SEGMENT_SEP)
                            .append(TokenConstants.TOKEN_TLS_PORT).append(TokenConstants.EQ)
                            .append(brokerTlsPort).toString();
                    strBuffer.delete(0, strBuffer.length());
                    inBrokerConfEntiyMap.put(inputKey, new BdbBrokerConfEntity(brokerId, brokerIp,
                            brokerPort, numPartitions, unflushThreshold, unflushInterval, deleteWhen,
                            deletePolicy, manageStatus, acceptPublish, acceptSubscribe, attributes,
                            true, false, itemCreateUser, itemCreateDate, itemCreateUser, itemCreateDate));
                } catch (Exception ee) {
                    strBuffer.delete(0, strBuffer.length());
                    throw new Exception(strBuffer.append("Process data exception, data is :")
                            .append(jsonObject.toString()).append(", exception is : ")
                            .append(ee.getMessage()).toString());
                }
            }
            for (BdbBrokerConfEntity brokerConfEntity : inBrokerConfEntiyMap.values()) {
                brokerConfManage.confAddBrokerDefaultConfig(brokerConfEntity);
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
     * Make broker config online
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminOnlineBrokerConf(
            HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate",
                            req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, new Date());
            Set<BdbBrokerConfEntity> bathBrokerEntitys =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"),
                            brokerConfManage, true, strBuffer);
            int manageStatus = TStatusConstants.STATUS_MANAGE_ONLINE;
            Map<Integer, BrokerInfo> oldBrokerInfoMap =
                    master.getBrokerHolder().getBrokerInfoMap();
            Set<BdbBrokerConfEntity> newBrokerEntitySet =
                    new HashSet<BdbBrokerConfEntity>();
            for (BdbBrokerConfEntity oldEntity : bathBrokerEntitys) {
                if (oldEntity == null) {
                    continue;
                }
                if (oldEntity.getManageStatus() == manageStatus) {
                    continue;
                }
                checkBrokerDuplicateRecord(oldEntity, strBuffer, oldBrokerInfoMap);
                if (WebParameterUtils.checkBrokerInProcessing(oldEntity.getBrokerId(), brokerConfManage, strBuffer)) {
                    throw new Exception(strBuffer.toString());
                }
                newBrokerEntitySet.add(new BdbBrokerConfEntity(oldEntity.getBrokerId(),
                        oldEntity.getBrokerIp(), oldEntity.getBrokerPort(),
                        oldEntity.getDftNumPartitions(), oldEntity.getDftUnflushThreshold(),
                        oldEntity.getDftUnflushInterval(), oldEntity.getDftDeleteWhen(),
                        oldEntity.getDftDeletePolicy(), manageStatus,
                        oldEntity.isAcceptPublish(), oldEntity.isAcceptSubscribe(),
                        oldEntity.getAttributes(), oldEntity.isConfDataUpdated(),
                        oldEntity.isBrokerLoaded(), oldEntity.getRecordCreateUser(),
                        oldEntity.getRecordCreateDate(), modifyUser, modifyDate));
            }
            for (BdbBrokerConfEntity newEntity : newBrokerEntitySet) {
                BdbBrokerConfEntity oldEntity =
                        brokerConfManage.getBrokerDefaultConfigStoreInfo(newEntity.getBrokerId());
                if (oldEntity == null
                        || oldEntity.getManageStatus() == newEntity.getManageStatus()
                        || WebParameterUtils.checkBrokerInProcessing(newEntity.getBrokerId(), brokerConfManage, null)) {
                    continue;
                }
                try {
                    boolean isNeedFastStart =
                            isBrokerStartNeedFast(brokerConfManage, newEntity.getBrokerId(),
                                    oldEntity.getManageStatus(), manageStatus);
                    brokerConfManage.confModBrokerDefaultConfig(newEntity);
                    brokerConfManage.triggerBrokerConfDataSync(newEntity,
                            oldEntity.getManageStatus(), isNeedFastStart);
                } catch (Exception e2) {
                    //
                }
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
     * Set read/write status of a broker.
     * The same operations could be made by changing broker's config,
     * but those are extracted here to simplify the code.
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminSetReadOrWriteBrokerConf(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate",
                            req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, new Date());
            String strIsAcceptPublish = req.getParameter("isAcceptPublish");
            String strIsAcceptSubscribe = req.getParameter("isAcceptSubscribe");
            if ((TStringUtils.isBlank(strIsAcceptPublish))
                    && (TStringUtils.isBlank(strIsAcceptSubscribe))) {
                throw new Exception("Required isAcceptPublish or isAcceptSubscribe parameter");
            }
            Set<BdbBrokerConfEntity> bathBrokerEntitySet = WebParameterUtils.getBatchBrokerIdSet(
                    req.getParameter("brokerId"), brokerConfManage, true, strBuffer);
            Map<Integer, BrokerInfo> oldBrokerInfoMap =
                    master.getBrokerHolder().getBrokerInfoMap();

            // 确认待修改broker状态的broker当前状态与预期状态信息,确认是否有修改
            // 如果有修改则检查当前broker状态是否满足修改,如果都满足则形成修改记录
            // Check the current status and status after the change, to see if there are changes.
            // If yes, check if the current status complies with the change.
            // If it complies, record the change.
            Set<BdbBrokerConfEntity> newBrokerEntitySet = new HashSet<BdbBrokerConfEntity>();
            for (BdbBrokerConfEntity oldEntity : bathBrokerEntitySet) {
                if (oldEntity == null) {
                    continue;
                }
                checkBrokerDuplicateRecord(oldEntity, strBuffer, oldBrokerInfoMap);
                boolean acceptPublish = false;
                boolean acceptSubscribe = false;
                if (oldEntity.getManageStatus() >= TStatusConstants.STATUS_MANAGE_ONLINE) {
                    if (oldEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_ONLINE) {
                        acceptPublish = true;
                        acceptSubscribe = true;
                    } else if (oldEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE) {
                        acceptPublish = false;
                        acceptSubscribe = true;
                    } else if (oldEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ) {
                        acceptPublish = true;
                        acceptSubscribe = false;
                    }
                }
                if (TStringUtils.isNotBlank(strIsAcceptPublish)) {
                    acceptPublish =
                            WebParameterUtils.validBooleanDataParameter("isAcceptPublish",
                                    req.getParameter("isAcceptPublish"), false, true);
                }
                if (TStringUtils.isNotBlank(strIsAcceptSubscribe)) {
                    acceptSubscribe =
                            WebParameterUtils.validBooleanDataParameter("isAcceptSubscribe",
                                    req.getParameter("isAcceptSubscribe"), false, true);
                }
                int manageStatus = TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ;
                if ((acceptPublish) && (acceptSubscribe)) {
                    manageStatus = TStatusConstants.STATUS_MANAGE_ONLINE;
                } else if (!acceptPublish && !acceptSubscribe) {
                    if (oldEntity.getManageStatus() < TStatusConstants.STATUS_MANAGE_ONLINE) {
                        throw new Exception(strBuffer.append("Broker by brokerId=")
                                .append(oldEntity.getBrokerId())
                                .append(" on draft status, not need offline operate!").toString());
                    }
                    manageStatus = TStatusConstants.STATUS_MANAGE_OFFLINE;
                } else if (acceptSubscribe) {
                    manageStatus = TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE;
                }
                if ((manageStatus == TStatusConstants.STATUS_MANAGE_OFFLINE)
                        && (oldEntity.getManageStatus() < TStatusConstants.STATUS_MANAGE_ONLINE)) {
                    continue;
                }
                if (oldEntity.getManageStatus() == manageStatus) {
                    continue;
                }
                if (WebParameterUtils.checkBrokerInProcessing(oldEntity.getBrokerId(), brokerConfManage, strBuffer)) {
                    throw new Exception(strBuffer.toString());
                }
                newBrokerEntitySet.add(new BdbBrokerConfEntity(oldEntity.getBrokerId(),
                        oldEntity.getBrokerIp(), oldEntity.getBrokerPort(),
                        oldEntity.getDftNumPartitions(), oldEntity.getDftUnflushThreshold(),
                        oldEntity.getDftUnflushInterval(), oldEntity.getDftDeleteWhen(),
                        oldEntity.getDftDeletePolicy(), manageStatus,
                        oldEntity.isAcceptPublish(), oldEntity.isAcceptSubscribe(),
                        oldEntity.getAttributes(), oldEntity.isConfDataUpdated(),
                        oldEntity.isBrokerLoaded(), oldEntity.getRecordCreateUser(),
                        oldEntity.getRecordCreateDate(), modifyUser, modifyDate));
            }

            // Perform the change on status
            for (BdbBrokerConfEntity newEntity : newBrokerEntitySet) {
                BdbBrokerConfEntity oldEntity =
                        brokerConfManage.getBrokerDefaultConfigStoreInfo(newEntity.getBrokerId());
                if (oldEntity == null
                        || oldEntity.getManageStatus() == newEntity.getManageStatus()
                        || WebParameterUtils.checkBrokerInProcessing(newEntity.getBrokerId(), brokerConfManage, null)) {
                    continue;
                }
                try {
                    boolean isNeedFastStart =
                            isBrokerStartNeedFast(brokerConfManage, newEntity.getBrokerId(),
                                    oldEntity.getManageStatus(), newEntity.getManageStatus());
                    brokerConfManage.confModBrokerDefaultConfig(newEntity);
                    brokerConfManage.triggerBrokerConfDataSync(newEntity,
                            oldEntity.getManageStatus(), isNeedFastStart);
                } catch (Exception e2) {
                    //
                }
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
     * @param oldEntity
     * @param strBuffer
     * @param oldBrokerInfoMap
     * @throws Exception
     */
    private void checkBrokerDuplicateRecord(BdbBrokerConfEntity oldEntity, StringBuilder strBuffer,
                                            Map<Integer, BrokerInfo> oldBrokerInfoMap) throws Exception {
        if (oldEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_APPLY) {
            BrokerInfo tmpBrokerInfo = oldBrokerInfoMap.get(oldEntity.getBrokerId());
            if (tmpBrokerInfo != null) {
                throw new Exception(strBuffer
                        .append("Illegal value:  the brokerId (")
                        .append(oldEntity.getBrokerId())
                        .append(") is used by another broker, please quit the broker first! " +
                                "Using the brokerId's brokerIp=")
                        .append(tmpBrokerInfo.getHost()).toString());
            }
            for (BrokerInfo oldBrokerInfo : oldBrokerInfoMap.values()) {
                if (oldBrokerInfo.getHost().equals(oldEntity.getBrokerIp())) {
                    throw new Exception(strBuffer
                            .append("Illegal value: the brokerId's Ip is used by another broker, " +
                                    "please quit the broker first! BrokerIp=")
                            .append(oldEntity.getBrokerIp())
                            .append(",current brokerId=")
                            .append(oldEntity.getBrokerId())
                            .append(",using the brokerIp's brokerId=")
                            .append(oldBrokerInfo.getBrokerId()).toString());
                }
            }
        }
    }

    /**
     * Release broker auto forbidden status
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminRelBrokerAutoForbiddenStatus(
            HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate",
                            req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, new Date());
            Set<Integer> bathBrokerIds = new HashSet<Integer>();
            Set<BdbBrokerConfEntity> bathBrokerEntitySet = WebParameterUtils.getBatchBrokerIdSet(
                    req.getParameter("brokerId"), brokerConfManage, true, strBuffer);
            for (BdbBrokerConfEntity entity : bathBrokerEntitySet) {
                bathBrokerIds.add(entity.getBrokerId());
            }
            String relReason =
                    WebParameterUtils.validStringParameter("relReason",
                            req.getParameter("relReason"),
                            TBaseConstants.META_MAX_OPREASON_LENGTH,
                            false, "API call to release auto-forbidden brokers");
            BrokerInfoHolder brokerInfoHolder = master.getBrokerHolder();
            brokerInfoHolder.relAutoForbiddenBrokerInfo(bathBrokerIds, relReason);
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * Update broker default config.
     * The current record will be checked firstly. The update will be performed only when there are changes.
     *
     * @param req
     * @return
     * @throws Throwable
     */
    // #lizard forgives
    public StringBuilder adminUpdateBrokerConf(HttpServletRequest req) throws Throwable {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String modifyUser = WebParameterUtils.validStringParameter("modifyUser",
                    req.getParameter("modifyUser"), TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            Date modifyDate = WebParameterUtils.validDateParameter("modifyDate",
                    req.getParameter("modifyDate"), TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, new Date());
            Set<BdbBrokerConfEntity> bathBrokerEntitySet = WebParameterUtils.getBatchBrokerIdSet(
                    req.getParameter("brokerId"), brokerConfManage, true, strBuffer);
            Set<BdbBrokerConfEntity> modifyBdbEntitySet = new HashSet<BdbBrokerConfEntity>();

            // Check the entities one by one, to see if there are changes.
            for (BdbBrokerConfEntity oldEntity : bathBrokerEntitySet) {
                if (oldEntity == null) {
                    continue;
                }
                boolean foundChange = false;
                BdbBrokerConfEntity newEntity =
                        new BdbBrokerConfEntity(oldEntity.getBrokerId(), oldEntity.getBrokerIp(),
                                oldEntity.getBrokerPort(), oldEntity.getDftNumPartitions(),
                                oldEntity.getDftUnflushThreshold(), oldEntity.getDftUnflushInterval(),
                                oldEntity.getDftDeleteWhen(), oldEntity.getDftDeletePolicy(),
                                oldEntity.getManageStatus(), oldEntity.isAcceptPublish(),
                                oldEntity.isAcceptSubscribe(), oldEntity.getAttributes(), oldEntity.isConfDataUpdated(),
                                oldEntity.isBrokerLoaded(),
                                oldEntity.getRecordCreateUser(), oldEntity.getRecordCreateDate(),
                                modifyUser, modifyDate);
                String deleteWhen = WebParameterUtils.validDecodeStringParameter("deleteWhen",
                        req.getParameter("deleteWhen"), TServerConstants.CFG_DELETEWHEN_MAX_LENGTH, false, null);
                if ((!TStringUtils.isBlank(deleteWhen)) && (!deleteWhen.equals(oldEntity.getDftDeleteWhen()))) {
                    foundChange = true;
                    newEntity.setDftDeleteWhen(deleteWhen);
                }
                int brokerPort = WebParameterUtils.validIntDataParameter("brokerPort",
                        req.getParameter("brokerPort"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
                if ((brokerPort != TBaseConstants.META_VALUE_UNDEFINED)
                        && (oldEntity.getBrokerPort() != brokerPort)) {
                    foundChange = true;
                    newEntity.setBrokerIpAndPort(oldEntity.getBrokerIp(), brokerPort);
                }
                String deletePolicy = WebParameterUtils.validDecodeStringParameter("deletePolicy",
                        req.getParameter("deletePolicy"), TServerConstants.CFG_DELETEPOLICY_MAX_LENGTH, false, null);
                if ((!TStringUtils.isBlank(deletePolicy)) && (!deletePolicy.equals(oldEntity.getDftDeletePolicy()))) {
                    foundChange = true;
                    newEntity.setDftDeletePolicy(deletePolicy);
                }
                int numPartitions = WebParameterUtils.validIntDataParameter("numPartitions",
                        req.getParameter("numPartitions"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
                if ((numPartitions > 0) && (numPartitions != oldEntity.getDftNumPartitions())) {
                    foundChange = true;
                    newEntity.setDftNumPartitions(numPartitions);
                }
                int unflushThreshold = WebParameterUtils.validIntDataParameter("unflushThreshold",
                        req.getParameter("unflushThreshold"), false, TBaseConstants.META_VALUE_UNDEFINED, 0);
                if ((unflushThreshold >= 0) && (unflushThreshold != oldEntity.getDftUnflushThreshold())) {
                    foundChange = true;
                    newEntity.setDftUnflushThreshold(unflushThreshold);
                }
                int unflushInterval = WebParameterUtils.validIntDataParameter("unflushInterval",
                        req.getParameter("unflushInterval"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
                if ((unflushInterval > 0) && (unflushInterval != oldEntity.getDftUnflushInterval())) {
                    foundChange = true;
                    newEntity.setDftUnflushInterval(unflushInterval);
                }
                int numTopicStores = WebParameterUtils.validIntDataParameter("numTopicStores",
                        req.getParameter("numTopicStores"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
                if ((numTopicStores > 0) && (numTopicStores != oldEntity.getNumTopicStores())) {
                    foundChange = true;
                    newEntity.appendAttributes(TokenConstants.TOKEN_STORE_NUM, String.valueOf(numTopicStores));
                }
                int unFlushDataHold = unflushThreshold;
                int brokerTlsPort = WebParameterUtils.validIntDataParameter("brokerTLSPort",
                        req.getParameter("brokerTLSPort"), false, TBaseConstants.META_VALUE_UNDEFINED, 0);
                if (brokerTlsPort >= 0 && brokerTlsPort != oldEntity.getBrokerTLSPort()) {
                    foundChange = true;
                    newEntity.setBrokerTLSPort(brokerTlsPort);
                }
                if (unFlushDataHold >= 0 && unFlushDataHold != oldEntity.getDftUnFlushDataHold()) {
                    foundChange = true;
                    newEntity.setDftUnFlushDataHold(unFlushDataHold);
                }
                int memCacheMsgCntInK = WebParameterUtils.validIntDataParameter("memCacheMsgCntInK",
                        req.getParameter("memCacheMsgCntInK"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
                if ((memCacheMsgCntInK > 0) && (memCacheMsgCntInK != oldEntity.getDftMemCacheMsgCntInK())) {
                    foundChange = true;
                    newEntity.setDftMemCacheMsgCntInK(memCacheMsgCntInK);
                }
                int memCacheMsgSizeInMB = WebParameterUtils.validIntDataParameter("memCacheMsgSizeInMB",
                        req.getParameter("memCacheMsgSizeInMB"), false, TBaseConstants.META_VALUE_UNDEFINED, 2);
                memCacheMsgSizeInMB = memCacheMsgSizeInMB >= 2048 ? 2048 : memCacheMsgSizeInMB;
                if ((memCacheMsgSizeInMB > 0) && (memCacheMsgSizeInMB != oldEntity.getDftMemCacheMsgSizeInMB())) {
                    foundChange = true;
                    newEntity.setDftMemCacheMsgSizeInMB(memCacheMsgSizeInMB);
                }
                int memCacheFlushIntvl = WebParameterUtils.validIntDataParameter("memCacheFlushIntvl",
                        req.getParameter("memCacheFlushIntvl"), false, TBaseConstants.META_VALUE_UNDEFINED, 4000);
                if ((memCacheFlushIntvl > 0) && (memCacheFlushIntvl != oldEntity.getDftMemCacheFlushIntvl())) {
                    foundChange = true;
                    newEntity.setDftMemCacheFlushIntvl(memCacheFlushIntvl);
                    newEntity.appendAttributes(TokenConstants.TOKEN_MCACHE_FLUSH_INTVL,
                            String.valueOf(memCacheFlushIntvl));
                }
                String publishParaStr = req.getParameter("acceptPublish");
                if (!TStringUtils.isBlank(publishParaStr)) {
                    boolean acceptPublish = WebParameterUtils.validBooleanDataParameter("acceptPublish",
                            req.getParameter("acceptPublish"), true, true);
                    if (acceptPublish != oldEntity.isAcceptPublish()) {
                        foundChange = true;
                        newEntity.setDftAcceptPublish(acceptPublish);
                    }
                }
                String subscribeParaStr = req.getParameter("acceptSubscribe");
                if (!TStringUtils.isBlank(subscribeParaStr)) {
                    boolean acceptSubscribe = WebParameterUtils.validBooleanDataParameter("acceptSubscribe",
                            req.getParameter("acceptSubscribe"), true, true);
                    if (acceptSubscribe != oldEntity.isAcceptSubscribe()) {
                        foundChange = true;
                        newEntity.setDftAcceptSubscribe(acceptSubscribe);
                    }
                }
                if (!foundChange) {
                    continue;
                }
                newEntity.setConfDataUpdated();
                modifyBdbEntitySet.add(newEntity);
            }
            try {
                // Perform the updates only on those which are changed
                boolean result = false;
                for (BdbBrokerConfEntity itemEntity : modifyBdbEntitySet) {
                    result = brokerConfManage.confModBrokerDefaultConfig(itemEntity);
                    BrokerSyncStatusInfo brokerSyncStatusInfo =
                            brokerConfManage.getBrokerRunSyncStatusInfo(itemEntity.getBrokerId());
                    if (result) {
                        if (brokerSyncStatusInfo != null) {
                            brokerConfManage.updateBrokerConfChanged(itemEntity.getBrokerId(), true, true);
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
     * Reload broker config
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminReloadBrokerConf(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate",
                            req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, new Date());
            Set<BdbBrokerConfEntity> bathBrokerEntitys =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"),
                            brokerConfManage, true, strBuffer);
            for (BdbBrokerConfEntity oldEntity : bathBrokerEntitys) {
                if (oldEntity == null) {
                    continue;
                }
                if (!WebParameterUtils.checkBrokerInOnlineStatus(oldEntity)) {
                    strBuffer.append("The broker manage status by brokerId=")
                            .append(oldEntity.getBrokerId())
                            .append(" not in online status, can't reload this configure! ");
                    throw new Exception(strBuffer.toString());
                }
                if (WebParameterUtils.checkBrokerInProcessing(oldEntity.getBrokerId(), brokerConfManage, strBuffer)) {
                    throw new Exception(strBuffer.toString());
                }
            }
            for (BdbBrokerConfEntity oldEntity : bathBrokerEntitys) {
                if (oldEntity == null
                        || !WebParameterUtils.checkBrokerInOnlineStatus(oldEntity)
                        || WebParameterUtils.checkBrokerInProcessing(oldEntity.getBrokerId(), brokerConfManage, null)) {
                    continue;
                }
                try {
                    boolean isNeedFastStart =
                            isBrokerStartNeedFast(brokerConfManage, oldEntity.getBrokerId(),
                                    oldEntity.getManageStatus(), oldEntity.getManageStatus());
                    brokerConfManage.triggerBrokerConfDataSync(oldEntity,
                            oldEntity.getManageStatus(), isNeedFastStart);
                } catch (Exception ee) {
                    //
                }
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
     * Make broker config offline
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminOfflineBrokerConf(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate",
                            req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, new Date());
            int manageStatus = TStatusConstants.STATUS_MANAGE_OFFLINE;
            Set<BdbBrokerConfEntity> bathBrokerEntitys =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"),
                            brokerConfManage, true, strBuffer);
            Set<BdbBrokerConfEntity> newBrokerEntitys =
                    new HashSet<BdbBrokerConfEntity>();
            for (BdbBrokerConfEntity oldEntity : bathBrokerEntitys) {
                if (oldEntity == null) {
                    continue;
                }
                if (oldEntity.getManageStatus() < TStatusConstants.STATUS_MANAGE_ONLINE) {
                    throw new Exception(strBuffer.append("Broker by brokerId=")
                            .append(oldEntity.getBrokerId())
                            .append(" on draft status, not need offline operate!").toString());
                }
                if (oldEntity.getManageStatus() == manageStatus) {
                    continue;
                }
                if (WebParameterUtils.checkBrokerInProcessing(oldEntity.getBrokerId(), brokerConfManage, strBuffer)) {
                    throw new Exception(strBuffer.toString());
                }
                newBrokerEntitys.add(new BdbBrokerConfEntity(oldEntity.getBrokerId(),
                        oldEntity.getBrokerIp(), oldEntity.getBrokerPort(),
                        oldEntity.getDftNumPartitions(), oldEntity.getDftUnflushThreshold(),
                        oldEntity.getDftUnflushInterval(), oldEntity.getDftDeleteWhen(),
                        oldEntity.getDftDeletePolicy(), manageStatus,
                        oldEntity.isAcceptPublish(), oldEntity.isAcceptSubscribe(),
                        oldEntity.getAttributes(), oldEntity.isConfDataUpdated(),
                        oldEntity.isBrokerLoaded(), oldEntity.getRecordCreateUser(),
                        oldEntity.getRecordCreateDate(), modifyUser, modifyDate));
            }
            for (BdbBrokerConfEntity newEntity : newBrokerEntitys) {
                BdbBrokerConfEntity oldEntity =
                        brokerConfManage.getBrokerDefaultConfigStoreInfo(newEntity.getBrokerId());
                if (oldEntity == null
                        || oldEntity.getManageStatus() == manageStatus
                        || oldEntity.getManageStatus() < TStatusConstants.STATUS_MANAGE_ONLINE
                        || WebParameterUtils.checkBrokerInProcessing(oldEntity.getBrokerId(), brokerConfManage, null)) {
                    continue;
                }
                try {
                    boolean isNeedFastStart =
                            isBrokerStartNeedFast(brokerConfManage, oldEntity.getBrokerId(),
                                    oldEntity.getManageStatus(), oldEntity.getManageStatus());
                    brokerConfManage.confModBrokerDefaultConfig(newEntity);
                    brokerConfManage.triggerBrokerConfDataSync(newEntity, oldEntity.getManageStatus(),
                            isNeedFastStart);
                } catch (Exception ee) {
                    //
                }
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
     * Delete broker config
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminDeleteBrokerConfEntityInfo(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("modifyDate",
                            req.getParameter("modifyDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, new Date());
            boolean isReservedData =
                    WebParameterUtils.validBooleanDataParameter("isReservedData",
                            req.getParameter("isReservedData"),
                            false, false);
            Set<BdbBrokerConfEntity> bathBrokerEntitys =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"),
                            brokerConfManage, true, strBuffer);
            for (BdbBrokerConfEntity oldEntity : bathBrokerEntitys) {
                if (oldEntity == null) {
                    continue;
                }
                ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicEntityMap =
                        brokerConfManage.getBrokerTopicConfEntitySet(oldEntity.getBrokerId());
                if ((brokerTopicEntityMap != null)
                        && (!brokerTopicEntityMap.isEmpty())) {
                    if (isReservedData) {
                        for (Map.Entry<String, BdbTopicConfEntity> entry : brokerTopicEntityMap.entrySet()) {
                            if (entry.getValue() == null) {
                                continue;
                            }
                            if (entry.getValue().getAcceptPublish()
                                    || entry.getValue().getAcceptSubscribe()) {
                                throw new Exception(strBuffer.append("The topic ")
                                        .append(entry.getKey())
                                        .append("'s acceptPublish and acceptSubscribe parameters must be false " +
                                                "in broker=")
                                        .append(oldEntity.getBrokerId())
                                        .append(" before broker delete by reserve data method!").toString());
                            }
                        }
                    } else {
                        throw new Exception(strBuffer.append("Topic configure of broker by brokerId=")
                                .append(oldEntity.getBrokerId())
                                .append(" not deleted, please delete broker's topic configure first!").toString());
                    }
                }
                if (WebParameterUtils.checkBrokerInOnlineStatus(oldEntity)) {
                    throw new Exception(strBuffer.append("Broker's manage status is online by brokerId=")
                            .append(oldEntity.getBrokerId())
                            .append(", please offline first!").toString());
                }
                if (WebParameterUtils.checkBrokerInOfflining(oldEntity.getBrokerId(),
                        oldEntity.getManageStatus(), brokerConfManage, strBuffer)) {
                    throw new Exception(strBuffer.toString());
                }
            }
            for (BdbBrokerConfEntity oldEntity : bathBrokerEntitys) {
                if (oldEntity == null
                        || WebParameterUtils.checkBrokerInOnlineStatus(oldEntity)
                        || WebParameterUtils.checkBrokerInOfflining(oldEntity.getBrokerId(),
                        oldEntity.getManageStatus(), brokerConfManage, null)) {
                    continue;
                }
                ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicEntityMap =
                        brokerConfManage.getBrokerTopicConfEntitySet(oldEntity.getBrokerId());
                if ((brokerTopicEntityMap != null)
                        && (!brokerTopicEntityMap.isEmpty())) {
                    if (isReservedData) {
                        boolean needCancel = false;
                        for (Map.Entry<String, BdbTopicConfEntity> btEntity : brokerTopicEntityMap.entrySet()) {
                            if (btEntity.getValue() == null) {
                                continue;
                            }
                            if ((btEntity.getValue().getAcceptPublish())
                                    && (btEntity.getValue().getAcceptSubscribe())) {
                                needCancel = true;
                                break;
                            }
                        }
                        if (needCancel) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                try {
                    if (isReservedData) {
                        ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicConfMap =
                                brokerConfManage.getBrokerTopicConfEntitySet(oldEntity.getBrokerId());
                        if (brokerTopicConfMap != null) {
                            brokerConfManage.clearConfigureTopicEntityInfo(oldEntity.getBrokerId());
                        }
                    }
                    brokerConfManage.confDelBrokerConfig(oldEntity);
                } catch (Exception ee) {
                    //
                }
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
     * Query run status of broker
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminQueryBrokerRunStatusInfo(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            BdbBrokerConfEntity brokerConfEntity = new BdbBrokerConfEntity();
            boolean withDetail =
                    WebParameterUtils.validBooleanDataParameter("withDetail",
                            req.getParameter("withDetail"), false, false);
            Set<String> bathBrokerIps =
                    WebParameterUtils.getBatchBrokerIpSet(req.getParameter("brokerIp"), false);
            Set<Integer> bathBrokerIds =
                    WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"), false);
            boolean onlyAutoForbidden =
                    WebParameterUtils.validBooleanDataParameter("onlyAutoForbidden",
                            req.getParameter("onlyAutoForbidden"), false, false);
            boolean onlyEnableTLS =
                    WebParameterUtils.validBooleanDataParameter("onlyEnableTLS",
                            req.getParameter("onlyEnableTLS"), false, false);
            int count = 0;
            List<BdbBrokerConfEntity> brokerConfEntityList =
                    brokerConfManage.confGetBdbBrokerEntitySet(brokerConfEntity);
            BrokerInfoHolder brokerInfoHolder = master.getBrokerHolder();
            Map<Integer, BrokerInfoHolder.BrokerForbInfo> brokerForbInfoMap =
                    brokerInfoHolder.getAutoForbiddenBrokerMapInfo();
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"data\":[");
            for (BdbBrokerConfEntity entity : brokerConfEntityList) {
                if (((!bathBrokerIds.isEmpty()) && (!bathBrokerIds.contains(entity.getBrokerId())))
                        || ((!bathBrokerIps.isEmpty()) && (!bathBrokerIps.contains(entity.getBrokerIp())))) {
                    continue;
                }
                BrokerInfoHolder.BrokerForbInfo brokerForbInfo =
                        brokerForbInfoMap.get(entity.getBrokerId());
                if (onlyAutoForbidden && brokerForbInfo == null) {
                    continue;
                }
                BrokerInfo brokerInfo = brokerInfoHolder.getBrokerInfo(entity.getBrokerId());
                if (onlyEnableTLS && (brokerInfo == null || !brokerInfo.isEnableTLS())) {
                    continue;
                }
                if (count++ > 0) {
                    strBuffer.append(",");
                }
                int brokerManageStatus = entity.getManageStatus();
                String strManageStatus = WebParameterUtils.getBrokerManageStatusStr(brokerManageStatus);
                strBuffer.append("{\"brokerId\":").append(entity.getBrokerId())
                        .append(",\"brokerIp\":\"").append(entity.getBrokerIp())
                        .append("\",\"brokerPort\":").append(entity.getBrokerPort())
                        .append(",\"manageStatus\":\"").append(strManageStatus).append("\"");
                if (brokerInfo == null) {
                    strBuffer.append(",\"brokerTLSPort\":").append(entity.getBrokerTLSPort())
                            .append(",\"enableTLS\":\"-\"");
                } else {
                    strBuffer.append(",\"brokerTLSPort\":").append(entity.getBrokerTLSPort())
                            .append(",\"enableTLS\":").append(brokerInfo.isEnableTLS());
                }
                if (brokerForbInfo == null) {
                    strBuffer.append(",\"isAutoForbidden\":false");
                } else {
                    strBuffer.append(",\"isAutoForbidden\":true");
                }
                if (brokerManageStatus == TStatusConstants.STATUS_MANAGE_APPLY) {
                    strBuffer.append(",\"runStatus\":\"-\",\"subStatus\":\"-\"")
                            .append(",\"isConfChanged\":\"-\",\"isConfLoaded\":\"-\",\"isBrokerOnline\":\"-\"")
                            .append(",\"brokerVersion\":\"-\",\"acceptPublish\":\"-\",\"acceptSubscribe\":\"-\"");
                } else {
                    BrokerSyncStatusInfo brokerSyncStatusInfo =
                            brokerConfManage.getBrokerRunSyncStatusInfo(entity.getBrokerId());
                    if (brokerSyncStatusInfo == null) {
                        strBuffer.append(",\"runStatus\":\"unRegister\",\"subStatus\":\"-\"")
                                .append(",\"isConfChanged\":\"-\",\"isConfLoaded\":\"-\",\"isBrokerOnline\":\"-\"")
                                .append(",\"brokerVersion\":\"-\",\"acceptPublish\":\"-\",\"acceptSubscribe\":\"-\"");
                    } else {
                        boolean isAcceptPublish = false;
                        boolean isAcceptSubscribe = false;
                        int stepStatus = brokerSyncStatusInfo.getBrokerRunStatus();
                        if (brokerSyncStatusInfo.isBrokerOnline()) {
                            if (stepStatus == TStatusConstants.STATUS_SERVICE_UNDEFINED) {
                                strBuffer.append(",\"runStatus\":\"running\",\"subStatus\":\"idle\"");
                            } else {
                                strBuffer.append(",\"runStatus\":\"running\",\"subStatus\":\"processing_event\"," +
                                        "\"stepOp\":")
                                        .append(stepStatus);
                            }
                        } else {
                            if (stepStatus == TStatusConstants.STATUS_SERVICE_UNDEFINED) {
                                strBuffer.append(",\"runStatus\":\"notRegister\",\"subStatus\":\"idle\"");
                            } else {
                                strBuffer.append(",\"runStatus\":\"notRegister\",\"subStatus\":\"processing_event\"," +
                                        "\"stepOp\":")
                                        .append(stepStatus);
                            }
                        }
                        strBuffer.append(",\"isConfChanged\":\"").append(brokerSyncStatusInfo.isBrokerConfChaned())
                                .append("\",\"isConfLoaded\":\"").append(brokerSyncStatusInfo.isBrokerLoaded())
                                .append("\",\"isBrokerOnline\":\"").append(brokerSyncStatusInfo.isBrokerOnline())
                                .append("\"");
                        switch (brokerManageStatus) {
                            case TStatusConstants.STATUS_MANAGE_ONLINE: {
                                isAcceptPublish = false;
                                isAcceptSubscribe = false;
                                if (brokerSyncStatusInfo.isBrokerRegister()) {
                                    if (brokerSyncStatusInfo.isBrokerOnline()) {
                                        if ((stepStatus == TStatusConstants.STATUS_SERVICE_TOONLINE_WAIT_REGISTER)
                                                || (stepStatus == TStatusConstants.STATUS_SERVICE_TOONLINE_WAIT_ONLINE)
                                                || (stepStatus == TStatusConstants.STATUS_SERVICE_TOONLINE_ONLY_READ)) {
                                            isAcceptPublish = false;
                                            isAcceptSubscribe = true;
                                        } else {
                                            isAcceptPublish = true;
                                            isAcceptSubscribe = true;
                                        }
                                    }
                                }
                                break;
                            }
                            case TStatusConstants.STATUS_MANAGE_OFFLINE: {
                                isAcceptPublish = false;
                                isAcceptSubscribe = false;
                                if (brokerSyncStatusInfo.isBrokerRegister()) {
                                    if (brokerSyncStatusInfo.isBrokerOnline()) {
                                        if (stepStatus == TStatusConstants.STATUS_SERVICE_TOOFFLINE_NOT_WRITE) {
                                            isAcceptPublish = false;
                                            isAcceptSubscribe = true;
                                        }
                                    }
                                }
                                break;
                            }
                            case TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE: {
                                isAcceptPublish = false;
                                isAcceptSubscribe = true;
                                break;
                            }

                            case TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ: {
                                isAcceptPublish = true;
                                isAcceptSubscribe = false;
                                break;
                            }
                            default: {
                                //
                            }
                        }
                        strBuffer.append(",\"brokerVersion\":\"-\",\"acceptPublish\":\"")
                                .append(isAcceptPublish).append("\",\"acceptSubscribe\":\"")
                                .append(isAcceptSubscribe).append("\"");
                        if (withDetail) {
                            strBuffer = brokerSyncStatusInfo.toJsonString(strBuffer.append(","), false);
                        }
                    }
                }
                strBuffer.append("}");
            }
            strBuffer.append("],\"count\":").append(count).append("}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\",\"count\":0,\"data\":[]}");
        }
        return strBuffer;
    }

    /**
     * Query broker config
     *
     * @param req
     * @return
     * @throws Exception
     */
    // #lizard forgives
    public StringBuilder adminQueryBrokerDefConfEntityInfo(HttpServletRequest req) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        BdbBrokerConfEntity brokerConfEntity = new BdbBrokerConfEntity();
        try {
            brokerConfEntity
                    .setRecordCreateUser(WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"), TBaseConstants.META_MAX_USERNAME_LENGTH, false, null));
            brokerConfEntity
                    .setRecordModifyUser(WebParameterUtils.validStringParameter("modifyUser",
                            req.getParameter("modifyUser"), TBaseConstants.META_MAX_USERNAME_LENGTH, false, null));
            brokerConfEntity
                    .setDftDeleteWhen(WebParameterUtils.validDecodeStringParameter("deleteWhen",
                            req.getParameter("deleteWhen"), TServerConstants.CFG_DELETEWHEN_MAX_LENGTH, false, null));
            brokerConfEntity
                    .setDftDeletePolicy(WebParameterUtils.validDecodeStringParameter("deletePolicy",
                            req.getParameter("deletePolicy"), TServerConstants.CFG_DELETEPOLICY_MAX_LENGTH, false,
                            null));
            brokerConfEntity
                    .setDftNumPartitions(WebParameterUtils.validIntDataParameter("numPartitions",
                            req.getParameter("numPartitions"), false, TBaseConstants.META_VALUE_UNDEFINED, 1));
            brokerConfEntity
                    .setDftUnflushInterval(WebParameterUtils.validIntDataParameter("unflushInterval",
                            req.getParameter("unflushInterval"), false, TBaseConstants.META_VALUE_UNDEFINED, 1));
            brokerConfEntity
                    .setDftUnflushThreshold(WebParameterUtils.validIntDataParameter("unflushThreshold",
                            req.getParameter("unflushThreshold"), false, TBaseConstants.META_VALUE_UNDEFINED, 0));
            brokerConfEntity
                    .setBrokerIp(WebParameterUtils.validAddressParameter("brokerIp",
                            req.getParameter("brokerIp"), TBaseConstants.META_MAX_BROKER_IP_LENGTH, false, ""));
            boolean withTopic =
                    WebParameterUtils.validBooleanDataParameter("withTopic",
                            req.getParameter("withTopic"), false, false);
            int topicStatusId =
                    WebParameterUtils.validIntDataParameter("topicStatusId",
                            req.getParameter("topicStatusId"), false,
                            TBaseConstants.META_VALUE_UNDEFINED, TBaseConstants.META_VALUE_UNDEFINED);
            int numTopicStores =
                    WebParameterUtils.validIntDataParameter("numTopicStores",
                            req.getParameter("numTopicStores"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
            int memCacheMsgCntInK =
                    WebParameterUtils.validIntDataParameter("memCacheMsgCntInK",
                            req.getParameter("memCacheMsgCntInK"), false, TBaseConstants.META_VALUE_UNDEFINED, 1);
            int memCacheMsgSizeInMB =
                    WebParameterUtils.validIntDataParameter("memCacheMsgSizeInMB",
                            req.getParameter("memCacheMsgSizeInMB"), false, TBaseConstants.META_VALUE_UNDEFINED, 2);
            int memCacheFlushIntvl =
                    WebParameterUtils.validIntDataParameter("memCacheFlushIntvl",
                            req.getParameter("memCacheFlushIntvl"), false, TBaseConstants.META_VALUE_UNDEFINED, 4000);
            int brokerTlsPort =
                    WebParameterUtils.validIntDataParameter("brokerTLSPort",
                            req.getParameter("brokerTLSPort"), false, TBaseConstants.META_VALUE_UNDEFINED, 0);
            Boolean isInclude = null;
            Set<String> bathTopicNames =
                    WebParameterUtils.getBatchTopicNames(req.getParameter("topicName"), false, false, null, strBuffer);
            if (!bathTopicNames.isEmpty()) {
                isInclude =
                        WebParameterUtils.validBooleanDataParameter("isInclude",
                                req.getParameter("isInclude"), false, true);
            }
            Set<Integer> bathBrokerIds = WebParameterUtils.getBatchBrokerIdSet(req.getParameter("brokerId"), false);
            if (bathBrokerIds.size() == 1) {
                for (Integer brokerId : bathBrokerIds) {
                    brokerConfEntity.setBrokerId(brokerId);
                }
            }
            int count = 0;
            SimpleDateFormat formatter =
                    new SimpleDateFormat(TBaseConstants.META_TMP_DATE_VALUE);
            List<BdbBrokerConfEntity> brokerConfEntityList =
                    brokerConfManage.confGetBdbBrokerEntitySet(brokerConfEntity);
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"data\":[");
            for (BdbBrokerConfEntity entity : brokerConfEntityList) {
                int recordNumTopicStores = entity.getNumTopicStores();
                int recordMemCacheMsgCntInK = entity.getDftMemCacheMsgCntInK();
                int recordMemCacheMsgSizeInMB = entity.getDftMemCacheMsgSizeInMB();
                int recordMemCacheFlushIntvl = entity.getDftMemCacheFlushIntvl();
                int recordTLSPort = entity.getBrokerTLSPort();
                if (((!bathBrokerIds.isEmpty()) && (!bathBrokerIds.contains(entity.getBrokerId())))
                        || ((numTopicStores >= 0) && (numTopicStores != recordNumTopicStores))
                        || ((memCacheMsgCntInK >= 0) && (memCacheMsgCntInK != recordMemCacheMsgCntInK))
                        || ((memCacheMsgSizeInMB >= 0) && (memCacheMsgSizeInMB != recordMemCacheMsgSizeInMB))
                        || ((memCacheFlushIntvl >= 0) && (memCacheFlushIntvl != recordMemCacheFlushIntvl))
                        || ((brokerTlsPort >= 0) && (brokerTlsPort != recordTLSPort))) {
                    continue;
                }
                ConcurrentHashMap<String, BdbTopicConfEntity> bdbTopicConfEntityMap =
                        brokerConfManage.getBrokerTopicConfEntitySet(entity.getBrokerId());
                if (!isValidRecord(bathTopicNames, topicStatusId, isInclude, bdbTopicConfEntityMap)) {
                    continue;
                }
                if (count++ > 0) {
                    strBuffer.append(",");
                }
                strBuffer.append("{\"brokerId\":").append(entity.getBrokerId())
                        .append(",\"brokerIp\":\"").append(entity.getBrokerIp())
                        .append("\",\"brokerPort\":").append(entity.getBrokerPort())
                        .append(",\"numPartitions\":").append(entity.getDftNumPartitions())
                        .append(",\"numTopicStores\":").append(recordNumTopicStores)
                        .append(",\"unflushThreshold\":").append(entity.getDftUnflushThreshold())
                        .append(",\"unflushInterval\":").append(entity.getDftUnflushInterval())
                        .append(",\"unFlushDataHold\":").append(entity.getDftUnFlushDataHold())
                        .append(",\"memCacheMsgCntInK\":").append(recordMemCacheMsgCntInK)
                        .append(",\"memCacheMsgSizeInMB\":").append(recordMemCacheMsgSizeInMB)
                        .append(",\"memCacheFlushIntvl\":").append(recordMemCacheFlushIntvl)
                        .append(",\"deleteWhen\":\"").append(entity.getDftDeleteWhen())
                        .append("\",\"deletePolicy\":\"").append(entity.getDftDeletePolicy())
                        .append("\",\"acceptPublish\":").append(String.valueOf(entity.isAcceptPublish()))
                        .append(",\"acceptSubscribe\":").append(String.valueOf(entity.isAcceptSubscribe()))
                        .append(",\"createUser\":\"").append(entity.getRecordCreateUser())
                        .append("\",\"createDate\":\"").append(formatter.format(entity.getRecordCreateDate()))
                        .append("\",\"modifyUser\":\"").append(entity.getRecordModifyUser())
                        .append("\",\"modifyDate\":\"").append(formatter.format(entity.getRecordModifyDate()))
                        .append("\"");
                if (recordTLSPort >= 0) {
                    strBuffer.append(",\"hasTLSPort\":true,\"brokerTLSPort\":").append(recordTLSPort);
                } else {
                    strBuffer.append(",\"hasTLSPort\":false");
                }
                strBuffer = addTopicInfo(withTopic, strBuffer, formatter, bdbTopicConfEntityMap);
                strBuffer.append("}");
            }
            strBuffer.append("],\"count\":").append(count).append("}");
        } catch (Exception e) {
            logger.error(" adminQueryBrokerDefConfEntityInfo exception", e);
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\",\"count\":0,\"data\":[]}");
        }
        return strBuffer;
    }

    /**
     * Check if the record is valid
     *
     * @param bathTopicNames
     * @param topicStatusId
     * @param isInclude
     * @param bdbTopicConfEntityMap
     * @return
     */
    private boolean isValidRecord(final Set<String> bathTopicNames, int topicStatusId, Boolean isInclude,
                                  ConcurrentHashMap<String, BdbTopicConfEntity> bdbTopicConfEntityMap) {
        // 首先检查指定了topic并且要求进行topic区分,并且broker有topic记录时,按照业务指定的topic区分要求进行过滤
        if (!bathTopicNames.isEmpty() && isInclude != null) {
            if ((bdbTopicConfEntityMap == null) || (bdbTopicConfEntityMap.isEmpty())) {
                if (isInclude) {
                    return false;
                }
            } else {
                boolean filterInclude = false;
                Set<String> curTopics = bdbTopicConfEntityMap.keySet();
                if (isInclude) {
                    for (String inTopic : bathTopicNames) {
                        if (curTopics.contains(inTopic)) {
                            filterInclude = true;
                            break;
                        }
                    }
                } else {
                    filterInclude = true;
                    for (String inTopic : bathTopicNames) {
                        if (curTopics.contains(inTopic)) {
                            filterInclude = false;
                            break;
                        }
                    }
                }
                if (!filterInclude) {
                    return false;
                }
            }
        }
        // 然后按照指定的topic状态进行过滤
        // 符合状态要求的才会被认为有效
        // Filter according to the topic status
        if (topicStatusId == TBaseConstants.META_VALUE_UNDEFINED) {
            return true;
        } else {
            if ((bdbTopicConfEntityMap == null) || (bdbTopicConfEntityMap.isEmpty())) {
                return false;
            }
            for (BdbTopicConfEntity bdbTopicConfEntity : bdbTopicConfEntityMap.values()) {
                if (bdbTopicConfEntity.getTopicStatusId() == topicStatusId) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Private method to add topic info
     *
     * @param withTopic
     * @param sb
     * @param formatter
     * @param bdbTopicConfEntityMap
     * @return
     */
    private StringBuilder addTopicInfo(boolean withTopic, StringBuilder sb, final SimpleDateFormat formatter,
                                       ConcurrentHashMap<String, BdbTopicConfEntity> bdbTopicConfEntityMap) {
        if (withTopic) {
            sb.append(",\"topicSet\":[");
            int topicCount = 0;
            if (bdbTopicConfEntityMap != null) {
                for (BdbTopicConfEntity topicEntity : bdbTopicConfEntityMap.values()) {
                    if (topicCount++ > 0) {
                        sb.append(",");
                    }
                    sb.append("{\"topicName\":\"").append(topicEntity.getTopicName())
                            .append("\",\"topicStatusId\":").append(topicEntity.getTopicStatusId())
                            .append(",\"brokerId\":").append(topicEntity.getBrokerId())
                            .append(",\"brokerIp\":\"").append(topicEntity.getBrokerIp())
                            .append("\",\"brokerPort\":").append(topicEntity.getBrokerPort())
                            .append(",\"numTopicStores\":").append(topicEntity.getNumTopicStores())
                            .append(",\"numPartitions\":").append(topicEntity.getNumPartitions())
                            .append(",\"unflushThreshold\":").append(topicEntity.getUnflushThreshold())
                            .append(",\"unflushInterval\":").append(topicEntity.getUnflushInterval())
                            .append(",\"unFlushDataHold\":").append(topicEntity.getunFlushDataHold())
                            .append(",\"memCacheMsgCntInK\":").append(topicEntity.getMemCacheMsgCntInK())
                            .append(",\"memCacheMsgSizeInMB\":").append(topicEntity.getMemCacheMsgSizeInMB())
                            .append(",\"memCacheFlushIntvl\":").append(topicEntity.getMemCacheFlushIntvl())
                            .append(",\"deleteWhen\":\"").append(topicEntity.getDeleteWhen())
                            .append("\",\"deletePolicy\":\"").append(topicEntity.getDeletePolicy())
                            .append("\",\"acceptPublish\":").append(String.valueOf(topicEntity.getAcceptPublish()))
                            .append(",\"acceptSubscribe\":").append(String.valueOf(topicEntity.getAcceptSubscribe()))
                            .append(",\"createUser\":\"").append(topicEntity.getCreateUser())
                            .append("\",\"createDate\":\"").append(formatter.format(topicEntity.getCreateDate()))
                            .append("\",\"modifyUser\":\"").append(topicEntity.getModifyUser())
                            .append("\",\"modifyDate\":\"").append(formatter.format(topicEntity.getModifyDate()))
                            .append("\"}");
                }
            }
            sb.append("]");
        }
        return sb;
    }
}
