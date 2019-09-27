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

package com.tencent.tubemq.server.master.nodemanage.nodebroker;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.TopicInfo;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corerpc.exception.StandbyException;
import com.tencent.tubemq.server.Server;
import com.tencent.tubemq.server.common.TServerConstants;
import com.tencent.tubemq.server.common.TStatusConstants;
import com.tencent.tubemq.server.common.fileconfig.BDBConfig;
import com.tencent.tubemq.server.master.bdbstore.DefaultBdbStoreService;
import com.tencent.tubemq.server.master.bdbstore.MasterGroupStatus;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBlackGroupEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumeGroupSettingEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumerGroupEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicAuthControlEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import com.tencent.tubemq.server.master.web.model.ClusterGroupVO;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerConfManage implements Server {

    private static final Logger logger = LoggerFactory.getLogger(BrokerConfManage.class);
    private final BDBConfig bdbConfig;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentHashMap<Integer, String> brokersMap =
            new ConcurrentHashMap<Integer, String>();
    private final ConcurrentHashMap<Integer, String> brokersTLSMap =
            new ConcurrentHashMap<Integer, String>();

    private final MasterGroupStatus masterGroupStatus = new MasterGroupStatus();
    private ConcurrentHashMap<Integer/* brokerId */, BdbBrokerConfEntity> brokerConfStoreMap =
            new ConcurrentHashMap<Integer, BdbBrokerConfEntity>();
    private ConcurrentHashMap<Integer/* brokerId */, BrokerSyncStatusInfo> brokerRunSyncManageMap =
            new ConcurrentHashMap<Integer, BrokerSyncStatusInfo>();
    private ConcurrentHashMap<Integer/* brokerId */, ConcurrentHashMap<String/* topicName */, BdbTopicConfEntity>>
            brokerTopicEntityStoreMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, BdbTopicConfEntity>>();
    private ConcurrentHashMap<Integer/* brokerId */, ConcurrentHashMap<String/* topicName */, TopicInfo>>
            brokerRunTopicInfoStoreMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, TopicInfo>>();
    private volatile boolean isStarted = false;
    private volatile boolean isStopped = false;
    private DefaultBdbStoreService mBdbStoreManagerService;
    private ConcurrentHashMap<String, BdbTopicAuthControlEntity> topicAuthControlEnableMap =
            new ConcurrentHashMap<String, BdbTopicAuthControlEntity>();
    private ConcurrentHashMap<
            String /* topicName */,
            ConcurrentHashMap<String /* consumerGroup */, BdbConsumerGroupEntity>>
            consumerGroupTopicMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, BdbConsumerGroupEntity>>();
    private ConcurrentHashMap<
            String /* consumerGroup */,
            ConcurrentHashMap<String /* topicName */, BdbBlackGroupEntity>>
            blackGroupTopicMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, BdbBlackGroupEntity>>();
    private ConcurrentHashMap<
            String /* topicName */,
            ConcurrentHashMap<String /* consumerGroup */, BdbGroupFilterCondEntity>>
            groupFilterCondTopicMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, BdbGroupFilterCondEntity>>();
    private ConcurrentHashMap<String /* groupName */, BdbGroupFlowCtrlEntity> consumeGroupFlowCtrlMap =
            new ConcurrentHashMap<String, BdbGroupFlowCtrlEntity>();
    private ConcurrentHashMap<String /* consumeGroup */, BdbConsumeGroupSettingEntity> consumeGroupSettingMap =
            new ConcurrentHashMap<String, BdbConsumeGroupSettingEntity>();
    private AtomicLong brokerInfoCheckSum = new AtomicLong(System.currentTimeMillis());
    private long lastBrokerUpdatedTime = System.currentTimeMillis();
    private long serviceStartTime = System.currentTimeMillis();


    public BrokerConfManage(DefaultBdbStoreService mBdbStoreManagerService) {
        this.mBdbStoreManagerService = mBdbStoreManagerService;
        this.bdbConfig = mBdbStoreManagerService.getBdbConfig();
        this.brokerConfStoreMap = this.mBdbStoreManagerService.getBrokerConfigMap();
        for (BdbBrokerConfEntity entity : this.brokerConfStoreMap.values()) {
            updateBrokerMaps(entity);
        }
        this.brokerTopicEntityStoreMap =
                this.mBdbStoreManagerService.getBrokerTopicEntityMap();
        this.consumerGroupTopicMap =
                this.mBdbStoreManagerService.getConsumerGroupNameAccControlMap();
        this.blackGroupTopicMap =
                this.mBdbStoreManagerService.getBlackGroupNameAccControlMap();
        this.topicAuthControlEnableMap =
                this.mBdbStoreManagerService.getTopicAuthControlMap();
        this.groupFilterCondTopicMap =
                this.mBdbStoreManagerService.getGroupFilterCondAccControlMap();
        this.consumeGroupFlowCtrlMap =
                this.mBdbStoreManagerService.getGroupFlowCtrlMap();
        this.consumeGroupSettingMap =
                this.mBdbStoreManagerService.getConsumeGroupSettingMap();
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Master Status Check");
                    }
                });
    }

    /**
     * If this node is the master role
     *
     * @return true if is master role or else
     */
    public boolean isSelfMaster() {
        return mBdbStoreManagerService.isMaster();
    }

    public boolean isPrimaryNodeActived() {
        return mBdbStoreManagerService.isPrimaryNodeActived();
    }

    /**
     * Transfer master role to replica node
     *
     * @throws Exception
     */
    public void transferMaster() throws Exception {
        if (mBdbStoreManagerService.isMaster()
                && !mBdbStoreManagerService.isPrimaryNodeActived()) {
            mBdbStoreManagerService.transferMaster();
        }
    }

    public void clearBrokerRunSyncManageData() {
        if (!this.isStarted
                && this.isStopped) {
            return;
        }
        this.brokerRunSyncManageMap.clear();
    }

    public InetSocketAddress getMasterAddress() {
        return mBdbStoreManagerService.getMasterAddress();
    }

    public ClusterGroupVO getGroupAddressStrInfo() {
        return mBdbStoreManagerService.getGroupAddressStrInfo();
    }


    @Override
    public void start() throws Exception {
        if (isStarted) {
            return;
        }
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    MasterGroupStatus tmpGroupStatus =
                            mBdbStoreManagerService.getMasterGroupStatus(true);
                    if (tmpGroupStatus == null) {
                        masterGroupStatus.setMasterGroupStatus(false, false, false);
                    } else {
                        masterGroupStatus.setMasterGroupStatus(mBdbStoreManagerService.isMaster(),
                                tmpGroupStatus.isWritable(), tmpGroupStatus.isReadable());
                    }
                } catch (Throwable e) {
                    logger.error(new StringBuilder(512)
                            .append("BDBGroupStatus Check exception, wait ")
                            .append(bdbConfig.getBdbStatusCheckTimeoutMs())
                            .append(" ms to try again.").append(e.getMessage()).toString());
                }
            }
        }, 0, bdbConfig.getBdbStatusCheckTimeoutMs(), TimeUnit.MILLISECONDS);
        for (BdbBrokerConfEntity brokerConfEntity : this.brokerConfStoreMap.values()) {
            updateBrokerMaps(brokerConfEntity);
            if (brokerConfEntity.getManageStatus() > TStatusConstants.STATUS_MANAGE_APPLY) {
                boolean needFastStart = false;
                BrokerSyncStatusInfo brokerSyncStatusInfo =
                        this.brokerRunSyncManageMap.get(brokerConfEntity.getBrokerId());
                List<String> brokerTopicSetConfInfo = getBrokerTopicStrConfigInfo(brokerConfEntity);
                if (brokerSyncStatusInfo == null) {
                    brokerSyncStatusInfo =
                            new BrokerSyncStatusInfo(brokerConfEntity, brokerTopicSetConfInfo);
                    BrokerSyncStatusInfo tmpBrokerSyncStatusInfo =
                            brokerRunSyncManageMap.putIfAbsent(brokerConfEntity.getBrokerId(),
                                    brokerSyncStatusInfo);
                    if (tmpBrokerSyncStatusInfo != null) {
                        brokerSyncStatusInfo = tmpBrokerSyncStatusInfo;
                    }
                }
                if (brokerTopicSetConfInfo.isEmpty()) {
                    needFastStart = true;
                }
                brokerSyncStatusInfo.setFastStart(needFastStart);
                brokerSyncStatusInfo.updateCurrBrokerConfInfo(brokerConfEntity.getManageStatus(),
                        brokerConfEntity.isConfDataUpdated(), brokerConfEntity.isBrokerLoaded(),
                        brokerConfEntity.getBrokerDefaultConfInfo(), brokerTopicSetConfInfo, false);
            }
        }
        isStarted = true;
        serviceStartTime = System.currentTimeMillis();
        logger.info("BrokerConfManage StoreService Started");
    }

    @Override
    public void stop() throws Exception {
        if (isStopped) {
            return;
        }
        this.scheduledExecutorService.shutdownNow();
        isStopped = true;
        logger.info("BrokerConfManage StoreService stopped");
    }

    public long getBrokerInfoCheckSum() {
        return this.brokerInfoCheckSum.get();
    }

    public ConcurrentHashMap<Integer, String> getBrokersMap(boolean isOverTLS) {
        if (isOverTLS) {
            return brokersTLSMap;
        } else {
            return brokersMap;
        }
    }

    /**
     * Check if consume target is authorization or not
     *
     * @param consumerId
     * @param consumerGroupName
     * @param reqTopicSet
     * @param reqTopicConditions
     * @param sb
     * @return
     */
    public TargetValidResult isConsumeTargetAuthorized(String consumerId,
                                                       String consumerGroupName,
                                                       Set<String> reqTopicSet,
                                                       Map<String, TreeSet<String>> reqTopicConditions,
                                                       final StringBuilder sb) {
        // #lizard forgives
        // check topic set
        if ((reqTopicSet == null) || (reqTopicSet.isEmpty())) {
            return new TargetValidResult(false, TErrCodeConstants.BAD_REQUEST,
                    "Request miss necessary subscribed topic data");
        }

        if ((reqTopicConditions != null) && (!reqTopicConditions.isEmpty())) {
            // check if request topic set is all in the filter topic set
            Set<String> condTopics = reqTopicConditions.keySet();
            List<String> unSetTopic = new ArrayList<String>();
            for (String topic : condTopics) {
                if (!reqTopicSet.contains(topic)) {
                    unSetTopic.add(topic);
                }
            }
            if (!unSetTopic.isEmpty()) {
                TargetValidResult validResult =
                        new TargetValidResult(false, TErrCodeConstants.BAD_REQUEST,
                                sb.append("Filter's Topic not subscribed :")
                                        .append(unSetTopic).toString());
                sb.delete(0, sb.length());
                return validResult;
            }
        }
        // check if consumer group is in the blacklist
        List<String> fbdTopicList = new ArrayList<String>();
        ConcurrentHashMap<String, BdbBlackGroupEntity> blackGroupEntityMap =
                this.blackGroupTopicMap.get(consumerGroupName);
        if (blackGroupEntityMap != null) {
            for (String topicItem : reqTopicSet) {
                if (blackGroupEntityMap.containsKey(topicItem)) {
                    fbdTopicList.add(topicItem);
                }
            }
        }
        if (!fbdTopicList.isEmpty()) {
            return new TargetValidResult(false, TErrCodeConstants.CONSUME_GROUP_FORBIDDEN,
                    sb.append("[unAuthorized Group] ").append(consumerId)
                            .append("'s consumerGroup in blackList by administrator, topics : ")
                            .append(fbdTopicList).toString());
        }
        // check if topic enabled authorization
        ArrayList<String> enableAuthTopicList = new ArrayList<String>();
        ArrayList<String> unAuthTopicList = new ArrayList<String>();
        for (String topicItem : reqTopicSet) {
            if (TStringUtils.isBlank(topicItem)) {
                continue;
            }
            BdbTopicAuthControlEntity topicEntity =
                    this.topicAuthControlEnableMap.get(topicItem);
            if (topicEntity == null) {
                continue;
            }
            if (topicEntity.isEnableAuthControl()) {
                enableAuthTopicList.add(topicItem);
                //check if consumer group is allowed to consume
                ConcurrentHashMap<String, BdbConsumerGroupEntity> consumerGroupEntityMap =
                        this.consumerGroupTopicMap.get(topicItem);
                if (consumerGroupEntityMap == null) {
                    continue;
                }
                if (consumerGroupEntityMap.get(consumerGroupName) == null) {
                    unAuthTopicList.add(topicItem);
                }
            }
        }
        if (!unAuthTopicList.isEmpty()) {
            return new TargetValidResult(false, TErrCodeConstants.CONSUME_GROUP_FORBIDDEN,
                    sb.append("[unAuthorized Group] ").append(consumerId)
                            .append("'s consumerGroup not authorized by administrator, unAuthorizedTopics : ")
                            .append(unAuthTopicList).toString());
        }
        if (enableAuthTopicList.isEmpty()) {
            return new TargetValidResult(true, 200, "Ok!");
        }
        boolean isAllowed =
                checkRestrictedTopics(consumerGroupName,
                        consumerId, enableAuthTopicList, reqTopicConditions, sb);
        if (isAllowed) {
            return new TargetValidResult(true, 200, "Ok!");
        } else {
            return new TargetValidResult(false,
                    TErrCodeConstants.CONSUME_CONTENT_FORBIDDEN, sb.toString());
        }
    }

    private boolean checkRestrictedTopics(final String groupName, final String consumerId,
                                          List<String> enableAuthTopicList,
                                          Map<String, TreeSet<String>> reqTopicConditions,
                                          final StringBuilder sb) {
        List<String> restrictedTopics = new ArrayList<>();
        Map<String, BdbGroupFilterCondEntity> authorizedFilterCondMap = new HashMap<>();
        for (String topicName : enableAuthTopicList) {
            ConcurrentHashMap<String, BdbGroupFilterCondEntity> groupFilterCondEntityMap =
                    this.groupFilterCondTopicMap.get(topicName);
            if (groupFilterCondEntityMap != null) {
                BdbGroupFilterCondEntity filterCondEntity =
                        groupFilterCondEntityMap.get(groupName);
                if (filterCondEntity != null && filterCondEntity.getControlStatus() > 1) {
                    restrictedTopics.add(topicName);
                    authorizedFilterCondMap.put(topicName, filterCondEntity);
                }
            }
        }
        if (restrictedTopics.isEmpty()) {
            return true;
        }
        boolean isAllowed = true;
        for (String tmpTopic : restrictedTopics) {
            BdbGroupFilterCondEntity bdbGroupFilterCondEntity =
                    authorizedFilterCondMap.get(tmpTopic);
            if (bdbGroupFilterCondEntity == null) {
                continue;
            }
            String allowedConds = bdbGroupFilterCondEntity.getAttributes();
            TreeSet<String> condItemSet = reqTopicConditions.get(tmpTopic);
            if (allowedConds.length() == 2
                    && allowedConds.equals(TServerConstants.TOKEN_BLANK_FILTER_CONDITION)) {
                isAllowed = false;
                sb.append("[Restricted Group] ")
                        .append(consumerId)
                        .append(" : ").append(groupName)
                        .append(" not allowed to consume any data of topic ")
                        .append(tmpTopic);
                break;
            } else {
                if (condItemSet == null
                        || condItemSet.isEmpty()) {
                    isAllowed = false;
                    sb.append("[Restricted Group] ")
                            .append(consumerId)
                            .append(" : ").append(groupName)
                            .append(" must set the filter conditions of topic ")
                            .append(tmpTopic);
                    break;
                } else {
                    Map<String, List<String>> unAuthorizedCondMap =
                            new HashMap<String, List<String>>();
                    for (String item : condItemSet) {
                        if (!allowedConds.contains(sb.append(TokenConstants.ARRAY_SEP)
                                .append(item).append(TokenConstants.ARRAY_SEP).toString())) {
                            isAllowed = false;
                            List<String> unAuthConds = unAuthorizedCondMap.get(tmpTopic);
                            if (unAuthConds == null) {
                                unAuthConds = new ArrayList<String>();
                                unAuthorizedCondMap.put(tmpTopic, unAuthConds);
                            }
                            unAuthConds.add(item);
                        }
                        sb.delete(0, sb.length());
                    }
                    if (!isAllowed) {
                        sb.append("[Restricted Group] ").append(consumerId)
                                .append(" : unAuthorized filter conditions ")
                                .append(unAuthorizedCondMap);
                        break;
                    }
                }
            }
        }
        return isAllowed;
    }


    // //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Store broker config
     *
     * @param bdbEntity
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confAddBrokerDefaultConfig(BdbBrokerConfEntity bdbEntity) throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        BdbBrokerConfEntity curEntity =
                this.brokerConfStoreMap.get(bdbEntity.getBrokerId());
        if (curEntity != null) {
            throw new Exception(strBuffer
                    .append("Duplicate add broker default info, exist record is: ")
                    .append(curEntity).toString());
        }
        BrokerSyncStatusInfo curSyncStatusInfo =
                this.brokerRunSyncManageMap.get(bdbEntity.getBrokerId());
        if (curSyncStatusInfo != null) {
            strBuffer.append("Duplicate broker configure in Run Sync manager, exist record is: ");
            throw new Exception(curSyncStatusInfo.toJsonString(strBuffer, true).toString());
        }
        boolean putResult = mBdbStoreManagerService.putBdbBrokerConfEntity(bdbEntity, true);
        if (putResult) {
            this.brokerConfStoreMap.put(bdbEntity.getBrokerId(), bdbEntity);
            updateBrokerMaps(bdbEntity);
            strBuffer.append("[confAddBrokerDefaultConfig  Success] ");
            logger.info(bdbEntity.toJsonString(strBuffer).toString());
            return true;
        }
        return false;
    }

    /**
     * Get broker config info
     *
     * @param bdbQueryEntity
     * @return broker config info list
     * @throws Exception
     */
    public List<BdbBrokerConfEntity> confGetBdbBrokerEntitySet(BdbBrokerConfEntity bdbQueryEntity)
            throws Exception {
        // #lizard forgives
        // find broker info
        validMasterStatus();
        List<BdbBrokerConfEntity> bdbBrokerEntities = new ArrayList<BdbBrokerConfEntity>();
        for (BdbBrokerConfEntity entity : brokerConfStoreMap.values()) {
            if (bdbQueryEntity == null) {
                bdbBrokerEntities.add(entity);
                continue;
            }
            // compare
            if (((bdbQueryEntity.getBrokerId() != TBaseConstants.META_VALUE_UNDEFINED)
                    && bdbQueryEntity.getBrokerId() != entity.getBrokerId())
                    || (!TStringUtils.isBlank(bdbQueryEntity.getBrokerIp())
                    && !bdbQueryEntity.getBrokerIp().equals(entity.getBrokerIp()))
                    || (!TStringUtils.isBlank(bdbQueryEntity.getBrokerAddress())
                    && !bdbQueryEntity.getBrokerAddress().equals(entity.getBrokerAddress()))
                    || (!TStringUtils.isBlank(bdbQueryEntity.getRecordCreateUser())
                    && !bdbQueryEntity.getRecordCreateUser().equals(entity.getRecordCreateUser()))
                    || (!TStringUtils.isBlank(bdbQueryEntity.getRecordModifyUser())
                    && !bdbQueryEntity.getRecordModifyUser().equals(entity.getRecordModifyUser()))
                    || (!TStringUtils.isBlank(bdbQueryEntity.getDftDeleteWhen())
                    && !bdbQueryEntity.getDftDeleteWhen().equals(entity.getDftDeleteWhen()))
                    || (!TStringUtils.isBlank(bdbQueryEntity.getDftDeletePolicy())
                    && !bdbQueryEntity.getDftDeletePolicy().equals(entity.getDftDeletePolicy()))
                    || (bdbQueryEntity.getBrokerPort() != TBaseConstants.META_VALUE_UNDEFINED
                    && bdbQueryEntity.getBrokerPort() != entity.getBrokerPort())
                    || (bdbQueryEntity.getDftNumPartitions() != TBaseConstants.META_VALUE_UNDEFINED
                    && bdbQueryEntity.getDftNumPartitions() != entity.getDftNumPartitions())
                    || (bdbQueryEntity.getDftUnflushInterval() != TBaseConstants.META_VALUE_UNDEFINED
                    && bdbQueryEntity.getDftUnflushInterval() != entity.getDftUnflushInterval())
                    || (bdbQueryEntity.getDftUnflushThreshold() != TBaseConstants.META_VALUE_UNDEFINED
                    && bdbQueryEntity.getDftUnflushThreshold() != entity.getDftUnflushThreshold())
                    || (bdbQueryEntity.getManageStatus() != TStatusConstants.STATUS_MANAGE_NOT_DEFINED
                    && bdbQueryEntity.getManageStatus() != entity.getManageStatus())) {
                continue;
            }
            // add to set
            bdbBrokerEntities.add(entity);
        }
        return bdbBrokerEntities;
    }

    /**
     * Delete broker config info
     *
     * @param bdbEntity
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confDelBrokerConfig(BdbBrokerConfEntity bdbEntity) throws Exception {
        validMasterStatus();
        BdbBrokerConfEntity oldBdbEntity =
                this.brokerConfStoreMap.get(bdbEntity.getBrokerId());
        BrokerSyncStatusInfo brokerSyncStatusInfo =
                this.brokerRunSyncManageMap.get(bdbEntity.getBrokerId());
        if ((oldBdbEntity == null) && (brokerSyncStatusInfo == null)) {
            logger.info("[confDelBrokerConfig  Success], not found record");
            return true;
        }
        ConcurrentHashMap<String/* topicName */, BdbTopicConfEntity> brokerTopicEntityMap =
                brokerTopicEntityStoreMap.get(bdbEntity.getBrokerId());
        if (brokerTopicEntityMap != null && !brokerTopicEntityMap.isEmpty()) {
            throw new Exception(
                    "Broker's topic configure not deleted, please delete broker's topic configure records first!");
        }
        if (oldBdbEntity != null && brokerSyncStatusInfo != null) {
            if (oldBdbEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_ONLINE
                    || oldBdbEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE
                    || oldBdbEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ) {
                throw new Exception("Broker manage status is online, please offline first!");
            }
            if (brokerSyncStatusInfo.isBrokerRegister()) {
                if (oldBdbEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_OFFLINE
                        && brokerSyncStatusInfo.getBrokerRunStatus() != TStatusConstants.STATUS_SERVICE_UNDEFINED) {
                    throw new Exception(
                            "Broker is processing offline event, please wait and try later!");
                }
            }
            mBdbStoreManagerService.delBdbBrokerConfEntity(bdbEntity.getBrokerId());
            this.brokerRunSyncManageMap.remove(bdbEntity.getBrokerId());
            this.brokerConfStoreMap.remove(bdbEntity.getBrokerId());
            logger.info("[confDelBrokerConfig  Success], found record");
            return true;
        } else {
            mBdbStoreManagerService.delBdbBrokerConfEntity(bdbEntity.getBrokerId());
            if (oldBdbEntity != null) {
                this.brokerConfStoreMap.remove(bdbEntity.getBrokerId());
            } else {
                StringBuilder strBuffer = new StringBuilder(1024)
                        .append("[Broker configure] RpcServer internal error: " +
                                "Broker bdb data not exist, run-status data is : ");
                logger.error(brokerSyncStatusInfo.toJsonString(strBuffer, true).toString());
                this.brokerRunSyncManageMap.remove(bdbEntity.getBrokerId());
            }
            return true;
        }
    }

    /**
     * Modify broker config
     *
     * @param bdbEntity
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confModBrokerDefaultConfig(BdbBrokerConfEntity bdbEntity) throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        BdbBrokerConfEntity oldBdbEntity =
                this.brokerConfStoreMap.get(bdbEntity.getBrokerId());
        if (oldBdbEntity == null) {
            throw new Exception(strBuffer.append("No broker configure in bdb store, broker is :")
                    .append(bdbEntity.getBrokerId()).append(":")
                    .append(bdbEntity.getBrokerAddress()).toString());
        }
        boolean putResult = mBdbStoreManagerService.putBdbBrokerConfEntity(bdbEntity, false);
        if (putResult) {
            this.brokerConfStoreMap.put(bdbEntity.getBrokerId(), bdbEntity);
            strBuffer.append("[confModBrokerDefaultConfig  Success] ");
            logger.info(bdbEntity.toJsonString(strBuffer).toString());
            return true;
        }
        return false;
    }

    /**
     * Manual reload broker config info
     *
     * @param bdbEntity
     * @param oldManageStatus
     * @param needFastStart
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean triggerBrokerConfDataSync(BdbBrokerConfEntity bdbEntity,
                                             int oldManageStatus,
                                             boolean needFastStart) throws Exception {
        validMasterStatus();
        BdbBrokerConfEntity curBdbEntity =
                this.brokerConfStoreMap.get(bdbEntity.getBrokerId());
        if (curBdbEntity == null) {
            throw new Exception(new StringBuilder(512)
                    .append("No broker configure in bdb store, broker is :")
                    .append(bdbEntity.getBrokerId()).append(":")
                    .append(bdbEntity.getBrokerAddress()).toString());
        }
        String curBrokerConfStr = curBdbEntity.getBrokerDefaultConfInfo();
        List<String> curBrokerTopicConfStrSet = getBrokerTopicStrConfigInfo(curBdbEntity);
        BrokerSyncStatusInfo brokerSyncStatusInfo =
                this.brokerRunSyncManageMap.get(bdbEntity.getBrokerId());
        if (brokerSyncStatusInfo == null) {
            brokerSyncStatusInfo =
                    new BrokerSyncStatusInfo(curBdbEntity, curBrokerTopicConfStrSet);
            BrokerSyncStatusInfo tmpBrokerSyncStatusInfo =
                    brokerRunSyncManageMap.putIfAbsent(curBdbEntity.getBrokerId(), brokerSyncStatusInfo);
            if (tmpBrokerSyncStatusInfo != null) {
                brokerSyncStatusInfo = tmpBrokerSyncStatusInfo;
            }
        }
        if (brokerSyncStatusInfo.isBrokerRegister()
                && brokerSyncStatusInfo.getBrokerRunStatus() != TStatusConstants.STATUS_SERVICE_UNDEFINED) {
            throw new Exception(new StringBuilder(512)
                    .append("The broker is processing online event(")
                    .append(brokerSyncStatusInfo.getBrokerRunStatus())
                    .append("), please try later! ").toString());
        }
        if (brokerSyncStatusInfo.isFastStart()) {
            brokerSyncStatusInfo.setFastStart(needFastStart);
        }
        int curManageStatus = curBdbEntity.getManageStatus();
        if (curManageStatus == TStatusConstants.STATUS_MANAGE_ONLINE
                || curManageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE
                || curManageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ) {
            boolean isOnlineUpdate =
                    (oldManageStatus == TStatusConstants.STATUS_MANAGE_ONLINE
                            || oldManageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE
                            || oldManageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ);
            brokerSyncStatusInfo.updateCurrBrokerConfInfo(curManageStatus,
                    curBdbEntity.isConfDataUpdated(), curBdbEntity.isBrokerLoaded(), curBrokerConfStr,
                    curBrokerTopicConfStrSet, isOnlineUpdate);
        } else {
            brokerSyncStatusInfo.setBrokerOffline();
        }
        StringBuilder strBuffer =
                new StringBuilder(1024).append("triggered broker syncStatus info is ");
        logger.info(brokerSyncStatusInfo.toJsonString(strBuffer, false).toString());
        return true;
    }

    /**
     * Remove broker and related topic list
     *
     * @param brokerId
     * @param removedTopics
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean clearRemovedTopicEntityInfo(int brokerId,
                                               List<String> removedTopics) throws Exception {
        if (removedTopics != null) {
            validMasterStatus();
            ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicConfMap =
                    this.brokerTopicEntityStoreMap.get(brokerId);
            if (brokerTopicConfMap != null) {
                for (String topicName : removedTopics) {
                    BdbTopicConfEntity storedTopicConf = brokerTopicConfMap.get(topicName);
                    if (storedTopicConf != null
                            && storedTopicConf.getTopicStatusId() == TStatusConstants.STATUS_TOPIC_SOFT_REMOVE) {
                        mBdbStoreManagerService
                                .delBdbTopicConfEntity(storedTopicConf.getRecordKey(),
                                        storedTopicConf.getTopicName());
                        brokerTopicConfMap.remove(topicName);
                        if (brokerTopicConfMap.isEmpty()) {
                            brokerTopicEntityStoreMap.remove(storedTopicConf.getBrokerId());
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Find the broker and delete all topic info in the broker
     *
     * @param brokerId
     * @return true if success
     * @throws Exception
     */
    public boolean clearConfigureTopicEntityInfo(int brokerId) throws Exception {
        validMasterStatus();
        ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicConfMap =
                this.brokerTopicEntityStoreMap.get(brokerId);
        if (brokerTopicConfMap != null) {
            for (BdbTopicConfEntity storedTopicConf : brokerTopicConfMap.values()) {
                if (storedTopicConf != null) {
                    mBdbStoreManagerService
                            .delBdbTopicConfEntity(storedTopicConf.getRecordKey(),
                                    storedTopicConf.getTopicName());
                    brokerTopicConfMap.remove(storedTopicConf.getTopicName());
                    if (brokerTopicConfMap.isEmpty()) {
                        brokerTopicEntityStoreMap.remove(storedTopicConf.getBrokerId());
                    }
                }
            }
        }
        return true;
    }

    public Set<String> getTotalConfiguredTopicNames() {
        Set<String> totalTopics = new HashSet<String>(50);
        for (ConcurrentHashMap<String, BdbTopicConfEntity> tmpTopicCfgMap
                : this.brokerTopicEntityStoreMap.values()) {
            if (tmpTopicCfgMap != null) {
                Set<String> tmpTopics = tmpTopicCfgMap.keySet();
                if (!tmpTopics.isEmpty()) {
                    totalTopics.addAll(tmpTopics);
                }
            }
        }
        return totalTopics;
    }


    public ConcurrentHashMap<Integer, BrokerSyncStatusInfo> getBrokerRunSyncManageMap() {
        return this.brokerRunSyncManageMap;
    }

    public BrokerSyncStatusInfo getBrokerRunSyncStatusInfo(int brokerId) {
        return this.brokerRunSyncManageMap.get(brokerId);
    }

    public BdbBrokerConfEntity getBrokerDefaultConfigStoreInfo(int brokerId) {
        return this.brokerConfStoreMap.get(brokerId);
    }

    public ConcurrentHashMap<Integer, BdbBrokerConfEntity> getBrokerConfStoreMap() {
        return this.brokerConfStoreMap;
    }

    public ConcurrentHashMap<String, BdbTopicConfEntity> getBrokerTopicConfEntitySet(
            final int brokerId) {
        return this.brokerTopicEntityStoreMap.get(brokerId);
    }

    public ConcurrentHashMap<String/* topicName */, TopicInfo> getBrokerRunTopicInfoMap(
            final int brokerId) {
        return this.brokerRunTopicInfoStoreMap.get(brokerId);
    }

    public void removeBrokerRunTopicInfoMap(final int brokerId) {
        this.brokerRunTopicInfoStoreMap.remove(brokerId);
    }

    public void updateBrokerRunTopicInfoMap(final int brokerId,
                                            ConcurrentHashMap<String, TopicInfo> topicInfoMap) {
        this.brokerRunTopicInfoStoreMap.put(brokerId, topicInfoMap);
    }

    public void resetBrokerReportInfo(final int brokerId) {
        BrokerSyncStatusInfo brokerSyncStatusInfo =
                brokerRunSyncManageMap.get(brokerId);
        if (brokerSyncStatusInfo != null) {
            brokerSyncStatusInfo.resetBrokerReportInfo();
        }
        brokerRunTopicInfoStoreMap.remove(brokerId);
    }

    /**
     * Update broker config
     *
     * @param brokerId
     * @param isChanged
     * @param isFasterStart
     * @return true if success otherwise false
     */
    public boolean updateBrokerConfChanged(int brokerId,
                                           boolean isChanged,
                                           boolean isFasterStart) {
        BdbBrokerConfEntity curBdbEntity = this.brokerConfStoreMap.get(brokerId);
        if (curBdbEntity == null) {
            return false;
        }
        if (isChanged) {
            if (!curBdbEntity.isConfDataUpdated()) {
                curBdbEntity.setConfDataUpdated();
                mBdbStoreManagerService.putBdbBrokerConfEntity(curBdbEntity, false);
                brokerConfStoreMap.put(curBdbEntity.getBrokerId(), curBdbEntity);
            }
            if (curBdbEntity.getManageStatus() > TStatusConstants.STATUS_MANAGE_APPLY) {
                BrokerSyncStatusInfo brokerSyncStatusInfo =
                        brokerRunSyncManageMap.get(curBdbEntity.getBrokerId());
                if (brokerSyncStatusInfo == null) {
                    List<String> newBrokerTopicConfStrSet =
                            getBrokerTopicStrConfigInfo(curBdbEntity);
                    brokerSyncStatusInfo =
                            new BrokerSyncStatusInfo(curBdbEntity, newBrokerTopicConfStrSet);
                    BrokerSyncStatusInfo tmpBrokerSyncStatusInfo =
                            brokerRunSyncManageMap.putIfAbsent(curBdbEntity.getBrokerId(), brokerSyncStatusInfo);
                    if (tmpBrokerSyncStatusInfo != null) {
                        brokerSyncStatusInfo = tmpBrokerSyncStatusInfo;
                    }
                }
                if (brokerSyncStatusInfo.isFastStart()) {
                    brokerSyncStatusInfo.setFastStart(isFasterStart);
                }
                if (!brokerSyncStatusInfo.isBrokerConfChaned()) {
                    brokerSyncStatusInfo.setBrokerConfChaned();
                }
            }
        } else {
            if (curBdbEntity.isConfDataUpdated()) {
                curBdbEntity.setBrokerLoaded();
                mBdbStoreManagerService.putBdbBrokerConfEntity(curBdbEntity, false);
                brokerConfStoreMap.put(curBdbEntity.getBrokerId(), curBdbEntity);
            }
            if (curBdbEntity.getManageStatus() > TStatusConstants.STATUS_MANAGE_APPLY) {
                BrokerSyncStatusInfo brokerSyncStatusInfo =
                        brokerRunSyncManageMap.get(curBdbEntity.getBrokerId());
                if (brokerSyncStatusInfo == null) {
                    List<String> newBrokerTopicConfStrSet =
                            getBrokerTopicStrConfigInfo(curBdbEntity);
                    brokerSyncStatusInfo =
                            new BrokerSyncStatusInfo(curBdbEntity, newBrokerTopicConfStrSet);
                    BrokerSyncStatusInfo tmpBrokerSyncStatusInfo =
                            brokerRunSyncManageMap.putIfAbsent(curBdbEntity.getBrokerId(),
                                    brokerSyncStatusInfo);
                    if (tmpBrokerSyncStatusInfo != null) {
                        brokerSyncStatusInfo = tmpBrokerSyncStatusInfo;
                    }
                }
                if (brokerSyncStatusInfo.isBrokerConfChaned()) {
                    brokerSyncStatusInfo.setBrokerLoaded();
                    brokerSyncStatusInfo.setFastStart(isFasterStart);
                }
            }
        }
        return true;
    }

    public void updateBrokerMaps(final BdbBrokerConfEntity bdbEntity) {
        if (bdbEntity != null) {
            String brokerReg =
                    this.brokersMap.putIfAbsent(bdbEntity.getBrokerId(),
                            bdbEntity.getSimpleBrokerInfo());
            String brokerTLSReg =
                    this.brokersTLSMap.putIfAbsent(bdbEntity.getBrokerId(),
                            bdbEntity.getSimpleTLSBrokerInfo());
            if (brokerReg == null
                    || brokerTLSReg == null
                    || !brokerReg.equals(bdbEntity.getSimpleBrokerInfo())
                    || !brokerTLSReg.equals(bdbEntity.getSimpleTLSBrokerInfo())) {
                if (brokerReg != null
                        && !brokerReg.equals(bdbEntity.getSimpleBrokerInfo())) {
                    this.brokersMap.put(bdbEntity.getBrokerId(), bdbEntity.getSimpleBrokerInfo());
                }
                if (brokerTLSReg != null
                        && !brokerTLSReg.equals(bdbEntity.getSimpleTLSBrokerInfo())) {
                    this.brokersTLSMap.put(bdbEntity.getBrokerId(), bdbEntity.getSimpleTLSBrokerInfo());
                }
                this.lastBrokerUpdatedTime = System.currentTimeMillis();
                this.brokerInfoCheckSum.set(this.lastBrokerUpdatedTime);
            }
        }
    }

    // /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Get broker topic entity, if query entity is null, return all topic entity
     *
     * @param bdbQueryEntity
     * @return topic entity list
     */
    public ConcurrentHashMap<String, List<BdbTopicConfEntity>> getBdbTopicEntityMap(
            BdbTopicConfEntity bdbQueryEntity) {
        // #lizard forgives
        ConcurrentHashMap<String, List<BdbTopicConfEntity>> bdbTopicEntityMap =
                new ConcurrentHashMap<String, List<BdbTopicConfEntity>>();
        for (ConcurrentHashMap<String, BdbTopicConfEntity> topicEntityMap
                : brokerTopicEntityStoreMap.values()) {
            for (BdbTopicConfEntity entity : topicEntityMap.values()) {
                List<BdbTopicConfEntity> bdbTopicEntities =
                        bdbTopicEntityMap.get(entity.getTopicName());
                if (bdbQueryEntity == null) {
                    if (bdbTopicEntities == null) {
                        bdbTopicEntities = new ArrayList<BdbTopicConfEntity>();
                        bdbTopicEntityMap.put(entity.getTopicName(), bdbTopicEntities);
                    }
                    bdbTopicEntities.add(entity);
                    continue;
                }
                //compare field
                if ((bdbQueryEntity.getBrokerId() != TBaseConstants.META_VALUE_UNDEFINED
                        && bdbQueryEntity.getBrokerId() != entity.getBrokerId())
                        || (TStringUtils.isNotBlank(bdbQueryEntity.getBrokerIp())
                        && !bdbQueryEntity.getBrokerIp().equals(entity.getBrokerIp()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getBrokerAddress())
                        && !bdbQueryEntity.getBrokerAddress().equals(entity.getBrokerAddress()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getTopicName())
                        && !bdbQueryEntity.getTopicName().equals(entity.getTopicName()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getCreateUser())
                        && !bdbQueryEntity.getCreateUser().equals(entity.getCreateUser()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getModifyUser())
                        && !bdbQueryEntity.getModifyUser().equals(entity.getModifyUser()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getDeleteWhen())
                        && !bdbQueryEntity.getDeleteWhen().equals(entity.getDeleteWhen()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getDeletePolicy())
                        && !bdbQueryEntity.getDeletePolicy().equals(entity.getDeletePolicy()))
                        || (bdbQueryEntity.getNumTopicStores() != TBaseConstants.META_VALUE_UNDEFINED
                        && bdbQueryEntity.getNumTopicStores() != entity.getNumTopicStores())
                        || (bdbQueryEntity.getMemCacheMsgSizeInMB() != TBaseConstants.META_VALUE_UNDEFINED
                        && bdbQueryEntity.getMemCacheMsgSizeInMB() != entity.getMemCacheMsgSizeInMB())
                        || (bdbQueryEntity.getMemCacheMsgCntInK() != TBaseConstants.META_VALUE_UNDEFINED
                        && bdbQueryEntity.getMemCacheMsgCntInK() != entity.getMemCacheMsgCntInK())
                        || (bdbQueryEntity.getMemCacheFlushIntvl() != TBaseConstants.META_VALUE_UNDEFINED
                        && bdbQueryEntity.getMemCacheFlushIntvl() != entity.getMemCacheFlushIntvl())
                        || (bdbQueryEntity.getTopicStatusId() != TBaseConstants.META_VALUE_UNDEFINED
                        && bdbQueryEntity.getTopicStatusId() != entity.getTopicStatusId())
                        || (bdbQueryEntity.getUnflushInterval() != TBaseConstants.META_VALUE_UNDEFINED
                        && bdbQueryEntity.getUnflushInterval() != entity.getUnflushInterval())
                        || (bdbQueryEntity.getUnflushThreshold() != TBaseConstants.META_VALUE_UNDEFINED
                        && bdbQueryEntity.getUnflushThreshold() != entity.getUnflushThreshold())) {
                    continue;
                }
                if (bdbTopicEntities == null) {
                    bdbTopicEntities = new ArrayList<BdbTopicConfEntity>();
                    bdbTopicEntityMap.put(entity.getTopicName(), bdbTopicEntities);
                }
                bdbTopicEntities.add(entity);
            }
        }
        return bdbTopicEntityMap;
    }

    /**
     * Add topic config
     *
     * @param bdbEntity
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confAddTopicConfig(BdbTopicConfEntity bdbEntity) throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        BdbBrokerConfEntity brokerConfEntity =
                brokerConfStoreMap.get(bdbEntity.getBrokerId());
        if (brokerConfEntity == null) {
            throw new Exception(
                    "Broker's default configure not create, " +
                            "please create the broker's default configure record first!");
        }
        ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicConfMap =
                this.brokerTopicEntityStoreMap.get(bdbEntity.getBrokerId());
        if (brokerTopicConfMap != null) {
            BdbTopicConfEntity curEntity =
                    brokerTopicConfMap.get(bdbEntity.getTopicName());
            if (curEntity != null) {
                throw new Exception(strBuffer
                        .append("Duplicate add broker's topic configure, exist record is: ")
                        .append(curEntity).toString());
            }
        }
        boolean putResult =
                mBdbStoreManagerService.putBdbTopicConfEntity(bdbEntity, true);
        if (putResult) {
            if (brokerTopicConfMap == null) {
                brokerTopicConfMap =
                        new ConcurrentHashMap<String, BdbTopicConfEntity>();
                ConcurrentHashMap<String, BdbTopicConfEntity> tmpBrokerTopicConfMap =
                        brokerTopicEntityStoreMap.putIfAbsent(bdbEntity.getBrokerId(), brokerTopicConfMap);
                if (tmpBrokerTopicConfMap != null) {
                    brokerTopicConfMap = tmpBrokerTopicConfMap;
                }
            }
            brokerTopicConfMap.put(bdbEntity.getTopicName(), bdbEntity);
            strBuffer.append("[confAddTopicConfig  Success] add broker's topic configure is :");
            logger.info(bdbEntity.toJsonString(strBuffer).toString());
            return true;
        }
        return false;
    }

    /**
     * Modify topic config
     *
     * @param bdbEntity
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confModTopicConfig(BdbTopicConfEntity bdbEntity) throws Exception {
        validMasterStatus();
        BrokerInfo brokerInfo =
                new BrokerInfo(bdbEntity.getBrokerId(),
                        bdbEntity.getBrokerIp(),
                        bdbEntity.getBrokerPort());
        StringBuilder strBuffer = new StringBuilder(512);
        ConcurrentHashMap<String, BdbTopicConfEntity> brokerTopicConfMap =
                this.brokerTopicEntityStoreMap.get(bdbEntity.getBrokerId());
        if (brokerTopicConfMap == null) {
            throw new Exception(strBuffer
                    .append("No broker's topic configure in cache, broker is :")
                    .append(brokerInfo.toString()).toString());
        }
        BdbTopicConfEntity curEntity =
                brokerTopicConfMap.get(bdbEntity.getTopicName());
        if (curEntity == null) {
            throw new Exception(strBuffer
                    .append("No broker's topic configure info for the broker in cache, broker is :")
                    .append(brokerInfo.toString()).append(", topic is : ")
                    .append(bdbEntity.getTopicName()).toString());
        }
        boolean putResult = mBdbStoreManagerService.putBdbTopicConfEntity(bdbEntity, false);
        if (putResult) {
            brokerTopicConfMap.put(bdbEntity.getTopicName(), bdbEntity);
            strBuffer.append("[confAddTopicConfig  Success] add broker's topic configure is :");
            logger.info(bdbEntity.toJsonString(strBuffer).toString());
            return true;
        }
        return false;
    }

    public List<String> getBrokerTopicStrConfigInfo(BdbBrokerConfEntity brokerConfEntity) {
        return innGetTopicStrConfigInfo(brokerConfEntity, false);
    }

    public List<String> getBrokerRemovedTopicStrConfigInfo(BdbBrokerConfEntity brokerConfEntity) {
        return innGetTopicStrConfigInfo(brokerConfEntity, true);
    }

    private List<String> innGetTopicStrConfigInfo(BdbBrokerConfEntity brokerConfEntity,
                                                  boolean isRemoved) {
        List<String> brokerTopicStrConfSet = new ArrayList<String>();
        ConcurrentHashMap<String, BdbTopicConfEntity> topicBdbEntytyMap =
                brokerTopicEntityStoreMap.get(brokerConfEntity.getBrokerId());
        if (topicBdbEntytyMap != null) {
            int defNumTopicStores = brokerConfEntity.getNumTopicStores();
            int defunFlushDataHold = brokerConfEntity.getDftUnFlushDataHold();
            int defmemCacheMsgSizeInMB = brokerConfEntity.getDftMemCacheMsgSizeInMB();
            int defmemCacheMsgCntInK = brokerConfEntity.getDftMemCacheMsgCntInK();
            int defMemCacheFlushIntvl = brokerConfEntity.getDftMemCacheFlushIntvl();
            StringBuilder sbuffer = new StringBuilder(512);
            for (BdbTopicConfEntity topicEntity : topicBdbEntytyMap.values()) {
                /*
                 * topic:partNum:acceptPublish:acceptSubscribe:unflushThreshold:unflushInterval:deleteWhen:
                 * deletePolicy:filterStatusId:statusId
                 */
                if (isRemoved) {
                    if (topicEntity.getTopicStatusId() != TStatusConstants.STATUS_TOPIC_SOFT_REMOVE) {
                        continue;
                    }
                } else {
                    if (topicEntity.getTopicStatusId() >= TStatusConstants.STATUS_TOPIC_SOFT_REMOVE) {
                        continue;
                    }
                }
                sbuffer.append(topicEntity.getTopicName());
                if (topicEntity.getNumPartitions() == brokerConfEntity.getDftNumPartitions()) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getNumPartitions());
                }
                if (topicEntity.getAcceptPublish() == brokerConfEntity.isAcceptPublish()) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getAcceptPublish());
                }
                if (topicEntity.getAcceptSubscribe() == brokerConfEntity.isAcceptSubscribe()) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getAcceptSubscribe());
                }
                if (topicEntity.getUnflushThreshold() == brokerConfEntity.getDftUnflushThreshold()) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getUnflushThreshold());
                }
                if (topicEntity.getUnflushInterval() == brokerConfEntity.getDftUnflushInterval()) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getUnflushInterval());
                }
                if (topicEntity.getDeleteWhen().equals(brokerConfEntity.getDftDeleteWhen())) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getDeleteWhen());
                }
                if (topicEntity.getDeletePolicy().equals(brokerConfEntity.getDftDeletePolicy())) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getDeletePolicy());
                }
                if (topicEntity.getNumTopicStores() == defNumTopicStores) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getNumTopicStores());
                }
                sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getTopicStatusId());
                if (topicEntity.getunFlushDataHold() == defunFlushDataHold) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getunFlushDataHold());
                }
                if (topicEntity.getMemCacheMsgSizeInMB() == defmemCacheMsgSizeInMB) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getMemCacheMsgSizeInMB());
                }
                if (topicEntity.getMemCacheMsgCntInK() == defmemCacheMsgCntInK) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getMemCacheMsgCntInK());
                }
                if (topicEntity.getMemCacheFlushIntvl() == defMemCacheFlushIntvl) {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(" ");
                } else {
                    sbuffer.append(TokenConstants.ATTR_SEP).append(topicEntity.getMemCacheFlushIntvl());
                }
                brokerTopicStrConfSet.add(sbuffer.toString());
                sbuffer.delete(0, sbuffer.length());
            }
        }
        return brokerTopicStrConfSet;
    }


    // /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Add or set topic auth control strategy
     *
     * @param bdbEntity
     * @return true if success
     * @throws Exception
     */
    public boolean confSetBdbTopicAuthControl(BdbTopicAuthControlEntity bdbEntity)
            throws Exception {
        validMasterStatus();
        boolean result =
                mBdbStoreManagerService.putBdbTopicAuthControlEntity(bdbEntity, true);
        topicAuthControlEnableMap.put(bdbEntity.getTopicName(), bdbEntity);
        StringBuilder strBuffer = new StringBuilder(512);
        if (result) {
            strBuffer.append("[confSetBdbTopicAuthControl  Success], add new record :");
        } else {
            strBuffer.append("[confSetBdbTopicAuthControl  Success], update old record :");
        }
        logger.info(bdbEntity.toJsonString(strBuffer).toString());
        return true;
    }

    /**
     * Delete topic auth control strategy
     *
     * @param bdbEntity
     * @return true if success
     * @throws Exception
     */
    public boolean confDeleteBdbTopicAuthControl(BdbTopicAuthControlEntity bdbEntity)
            throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        BdbTopicAuthControlEntity bdbTopicAuthControlEntity =
                this.topicAuthControlEnableMap.remove(bdbEntity.getTopicName());
        if (bdbTopicAuthControlEntity != null) {
            mBdbStoreManagerService.delBdbTopicAuthControlEntity(bdbEntity.getTopicName());
            strBuffer.append("[confDeleteBdbTopicAuthControl  Success], ")
                    .append(bdbEntity.getCreateUser()).append(" deleted record : ");
            logger.info(bdbTopicAuthControlEntity.toJsonString(strBuffer).toString());
        } else {
            logger.info(strBuffer.append("[confDeleteBdbTopicAuthControl  failure], ")
                    .append(bdbEntity.getCreateUser()).append(" not found record : ")
                    .append(bdbEntity.getTopicName()).toString());
        }
        return true;
    }

    /**
     * Get topic auth control entity list
     *
     * @param bdbQueryEntity
     * @return entity list
     * @throws Exception
     */
    public List<BdbTopicAuthControlEntity> confGetBdbTopicAuthCtrlEntityList(
            BdbTopicAuthControlEntity bdbQueryEntity) throws Exception {
        validMasterStatus();
        List<BdbTopicAuthControlEntity> bdbTopicAuthControlEntities =
                new ArrayList<BdbTopicAuthControlEntity>();
        for (BdbTopicAuthControlEntity entity : this.topicAuthControlEnableMap.values()) {
            if (bdbQueryEntity == null) {
                bdbTopicAuthControlEntities.add(entity);
                continue;
            }
            if ((!TStringUtils.isBlank(bdbQueryEntity.getTopicName())
                    && !bdbQueryEntity.getTopicName().equals(entity.getTopicName()))
                    || (bdbQueryEntity.getEnableAuthControl() != -1
                    && bdbQueryEntity.getEnableAuthControl() != entity.getEnableAuthControl())
                    || (!TStringUtils.isBlank(bdbQueryEntity.getCreateUser())
                    && !bdbQueryEntity.getCreateUser().equals(entity.getCreateUser()))) {
                continue;
            }
            bdbTopicAuthControlEntities.add(entity);
        }
        return bdbTopicAuthControlEntities;
    }

    public BdbTopicAuthControlEntity getBdbEnableAuthControlByTopicName(String topicName) {
        return this.topicAuthControlEnableMap.get(topicName);
    }

    // /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Add consumer group filter condition
     *
     * @param bdbEntity
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confAddNewGroupFilterCond(BdbGroupFilterCondEntity bdbEntity) throws Exception {
        validMasterStatus();
        ConcurrentHashMap<String, BdbGroupFilterCondEntity> groupFilterCondEntityMap =
                groupFilterCondTopicMap.get(bdbEntity.getTopicName());
        if (groupFilterCondEntityMap != null) {
            BdbGroupFilterCondEntity curEntity =
                    groupFilterCondEntityMap.get(bdbEntity.getConsumerGroupName());
            if (curEntity != null) {
                throw new Exception(new StringBuilder(512)
                        .append("Duplicate add allowed Group Filter Condition info, exist record is: ")
                        .append(curEntity.toString()).toString());
            }
        }
        boolean putResult =
                mBdbStoreManagerService.putBdbGroupFilterCondConfEntity(bdbEntity, true);
        if (putResult) {
            if (groupFilterCondEntityMap == null) {
                groupFilterCondEntityMap =
                        new ConcurrentHashMap<String, BdbGroupFilterCondEntity>();
                ConcurrentHashMap<String, BdbGroupFilterCondEntity> tmpGroupFilterCondEntityMap =
                        groupFilterCondTopicMap.putIfAbsent(bdbEntity.getTopicName(), groupFilterCondEntityMap);
                if (tmpGroupFilterCondEntityMap != null) {
                    groupFilterCondEntityMap = tmpGroupFilterCondEntityMap;
                }
            }
            groupFilterCondEntityMap.put(bdbEntity.getConsumerGroupName(), bdbEntity);
            logger.info(new StringBuilder(512).append("[confAddNewGroupFilterCond  Success] ")
                    .append(bdbEntity.toString()).toString());
            return true;
        }
        return false;
    }

    /**
     * Modify consumer group filter condition
     *
     * @param bdbEntity
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confModGroupFilterCondConfig(BdbGroupFilterCondEntity bdbEntity)
            throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        ConcurrentHashMap<String, BdbGroupFilterCondEntity> groupFilterCondEntityMap =
                groupFilterCondTopicMap.get(bdbEntity.getTopicName());
        if (groupFilterCondEntityMap == null) {
            throw new Exception(strBuffer
                    .append("No Topic's Group Filter Condition info in cache, modified info is :")
                    .append(bdbEntity.toString()).toString());
        }
        BdbGroupFilterCondEntity curEntity =
                groupFilterCondEntityMap.get(bdbEntity.getConsumerGroupName());
        if (curEntity == null) {
            throw new Exception(strBuffer
                    .append("No consumerGroupName's Filter Condition info, modified info is: ")
                    .append(bdbEntity.toString()).toString());
        }
        boolean putResult =
                mBdbStoreManagerService.putBdbGroupFilterCondConfEntity(bdbEntity, false);
        if (putResult) {
            groupFilterCondEntityMap.put(bdbEntity.getConsumerGroupName(), bdbEntity);
            strBuffer.append("[confModGroupFilterCond  Success] ");
            logger.info(bdbEntity.toJsonString(strBuffer).toString());
            return true;
        }
        return false;
    }

    /**
     * Get all allowed group filter condition for a specific topic
     *
     * @param topicName
     * @return consumer group filter list
     */
    public List<BdbGroupFilterCondEntity> getBdbAllowedGroupFilterConds(String topicName) {
        List<BdbGroupFilterCondEntity> bdbGroupFilterCondEntities =
                new ArrayList<BdbGroupFilterCondEntity>();
        ConcurrentHashMap<String, BdbGroupFilterCondEntity> groupFilterCondMap =
                groupFilterCondTopicMap.get(topicName);
        if (groupFilterCondMap != null) {
            bdbGroupFilterCondEntities.addAll(groupFilterCondMap.values());
        }
        return bdbGroupFilterCondEntities;
    }

    /**
     * Get group filter condition for a topic & group
     *
     * @param topicName the topic name to get group filter condition
     * @param groupName the group name to get group filter condition
     * @return consumer group filter
     */
    public BdbGroupFilterCondEntity getBdbAllowedGroupFilterConds(String topicName,
                                                                  String groupName) {
        ConcurrentHashMap<String, BdbGroupFilterCondEntity> groupFilterCondMap =
                groupFilterCondTopicMap.get(topicName);
        if (groupFilterCondMap != null) {
            return groupFilterCondMap.get(groupName);
        }
        return null;
    }

    /**
     * Query group filter condition for a entity, if
     *
     * @param bdbQueryEntity the entity to get group filter condition,
     *                       may be null, will return all group filter condition
     * @return group filter condition list
     * @throws Exception
     */
    public List<BdbGroupFilterCondEntity> confGetBdbAllowedGroupFilterCondSet(
            BdbGroupFilterCondEntity bdbQueryEntity) throws Exception {
        validMasterStatus();
        List<BdbGroupFilterCondEntity> bdbGroupFilterCondEntities =
                new ArrayList<BdbGroupFilterCondEntity>();
        for (ConcurrentHashMap<String, BdbGroupFilterCondEntity> groupFilterCondEntityMap
                : groupFilterCondTopicMap.values()) {
            for (BdbGroupFilterCondEntity entity : groupFilterCondEntityMap.values()) {
                if (bdbQueryEntity == null) {
                    bdbGroupFilterCondEntities.add(entity);
                    continue;
                }
                if ((!TStringUtils.isBlank(bdbQueryEntity.getRecordKey())
                        && !bdbQueryEntity.getRecordKey().equals(entity.getRecordKey()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getTopicName())
                        && !bdbQueryEntity.getTopicName().equals(entity.getTopicName()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getConsumerGroupName())
                        && !bdbQueryEntity.getConsumerGroupName().equals(entity.getConsumerGroupName()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getAttributes())
                        && !bdbQueryEntity.getAttributes().equals(entity.getAttributes()))
                        || (bdbQueryEntity.getControlStatus() != TStatusConstants.STATUS_SERVICE_UNDEFINED
                        && bdbQueryEntity.getControlStatus() != entity.getControlStatus())
                        || (!TStringUtils.isBlank(bdbQueryEntity.getCreateUser())
                        && !bdbQueryEntity.getCreateUser().equals(entity.getCreateUser()))) {
                    continue;
                }
                bdbGroupFilterCondEntities.add(entity);
            }
        }
        return bdbGroupFilterCondEntities;
    }

    /**
     * Delete group filter condition via bdb entity
     *
     * @param bdbEntity the entity will be deleted
     * @return true if success
     * @throws Exception
     */
    public boolean confDelBdbAllowedGroupFilterCondSet(BdbGroupFilterCondEntity bdbEntity)
            throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        ConcurrentHashMap<String, BdbGroupFilterCondEntity> groupFilterCondEntityMap =
                this.groupFilterCondTopicMap.get(bdbEntity.getTopicName());
        if (groupFilterCondEntityMap != null) {
            if (TStringUtils.isBlank(bdbEntity.getConsumerGroupName())) {
                for (BdbGroupFilterCondEntity bdbGroupFilterCondEntity
                        : groupFilterCondEntityMap.values()) {
                    mBdbStoreManagerService
                            .delBdbGroupFilterCondEntity(bdbGroupFilterCondEntity.getRecordKey());
                }
                this.groupFilterCondTopicMap.remove(bdbEntity.getTopicName());
                logger.info(strBuffer.append("[confDelGroupFilterCondConfig  Success], ")
                        .append(bdbEntity.getCreateUser()).append(" deleted all ")
                        .append(bdbEntity.getTopicName()).append("'s records").toString());
            } else {
                BdbGroupFilterCondEntity bdbGroupFilterEntity =
                        groupFilterCondEntityMap.remove(bdbEntity.getConsumerGroupName());
                if (bdbGroupFilterEntity != null) {
                    mBdbStoreManagerService
                            .delBdbGroupFilterCondEntity(bdbGroupFilterEntity.getRecordKey());
                    strBuffer.append("[confDelGroupFilterCondConfig  Success], ")
                            .append(bdbEntity.getCreateUser()).append(" deleted record : ");
                    logger.info(bdbGroupFilterEntity.toJsonString(strBuffer).toString());
                }
                if (groupFilterCondEntityMap.isEmpty()) {
                    this.groupFilterCondTopicMap.remove(bdbEntity.getTopicName());
                }
            }
        } else {
            logger.info("[confDelGroupFilterCondConfig  Success], not found record");
        }
        return true;
    }

    // /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Add allowed consumer group
     *
     * @param bdbEntity
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confAddAllowedConsumerGroup(BdbConsumerGroupEntity bdbEntity) throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        ConcurrentHashMap<String, BdbConsumerGroupEntity> consumerGroupEntityMap =
                consumerGroupTopicMap.get(bdbEntity.getGroupTopicName());
        if (consumerGroupEntityMap != null) {
            BdbConsumerGroupEntity curEntity =
                    consumerGroupEntityMap.get(bdbEntity.getConsumerGroupName());
            if (curEntity != null) {
                throw new Exception(strBuffer
                        .append("Duplicate add allowed consumerGroupName info, exist record is: ")
                        .append(curEntity).toString());
            }
        }
        boolean putResult =
                mBdbStoreManagerService.putBdbConsumerGroupConfEntity(bdbEntity, true);
        if (putResult) {
            if (consumerGroupEntityMap == null) {
                consumerGroupEntityMap = new ConcurrentHashMap<String, BdbConsumerGroupEntity>();
                ConcurrentHashMap<String, BdbConsumerGroupEntity> tmpConsumerGroupEntityMap =
                        consumerGroupTopicMap.putIfAbsent(bdbEntity.getGroupTopicName(), consumerGroupEntityMap);
                if (tmpConsumerGroupEntityMap != null) {
                    consumerGroupEntityMap = tmpConsumerGroupEntityMap;
                }
            }
            consumerGroupEntityMap.put(bdbEntity.getConsumerGroupName(), bdbEntity);
            strBuffer.append("[confAddAllowedConsumerGroup  Success] ");
            logger.info(bdbEntity.toJsonString(strBuffer).toString());
            return true;
        }
        return false;
    }

    /**
     * Query all allowed consumer group list
     *
     * @param topicName the topic name to query
     * @return consumer group list
     */
    public List<BdbConsumerGroupEntity> getBdbAllowedConsumerGroups(String topicName) {
        List<BdbConsumerGroupEntity> bdbConsumerGroupEntities =
                new ArrayList<BdbConsumerGroupEntity>();
        ConcurrentHashMap<String, BdbConsumerGroupEntity> consumerGroupMap =
                consumerGroupTopicMap.get(topicName);
        if (consumerGroupMap != null) {
            bdbConsumerGroupEntities.addAll(consumerGroupMap.values());
        }
        return bdbConsumerGroupEntities;
    }

    public List<BdbConsumerGroupEntity> confGetBdbAllowedConsumerGroupSet(
            BdbConsumerGroupEntity bdbQueryEntity) throws Exception {
        validMasterStatus();
        List<BdbConsumerGroupEntity> bdbConsumerGroupEntities =
                new ArrayList<BdbConsumerGroupEntity>();
        for (ConcurrentHashMap<String, BdbConsumerGroupEntity> consumerGroupEntityMap
                : consumerGroupTopicMap.values()) {
            for (BdbConsumerGroupEntity entity : consumerGroupEntityMap.values()) {
                if (bdbQueryEntity == null) {
                    bdbConsumerGroupEntities.add(entity);
                    continue;
                }
                if ((!TStringUtils.isBlank(bdbQueryEntity.getRecordKey())
                        && !bdbQueryEntity.getRecordKey().equals(entity.getRecordKey()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getGroupTopicName())
                        && !bdbQueryEntity.getGroupTopicName().equals(entity.getGroupTopicName()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getConsumerGroupName())
                        && !bdbQueryEntity.getConsumerGroupName().equals(entity.getConsumerGroupName()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getRecordCreateUser())
                        && !bdbQueryEntity.getRecordCreateUser().equals(entity.getRecordCreateUser()))) {
                    continue;
                }
                bdbConsumerGroupEntities.add(entity);
            }
        }
        return bdbConsumerGroupEntities;
    }

    /**
     * Delete consumer group from bdb
     *
     * @param bdbEntity the entity will be delete
     * @return true if success
     * @throws Exception
     */
    public boolean confDelBdbAllowedConsumerGroupSet(BdbConsumerGroupEntity bdbEntity)
            throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        ConcurrentHashMap<String, BdbConsumerGroupEntity> consumerGroupEntityMap =
                this.consumerGroupTopicMap.get(bdbEntity.getGroupTopicName());
        if (consumerGroupEntityMap != null) {
            if (TStringUtils.isBlank(bdbEntity.getConsumerGroupName())) {
                for (BdbConsumerGroupEntity bdbConsumerGroupEntity
                        : consumerGroupEntityMap.values()) {
                    mBdbStoreManagerService
                            .delBdbConsumerGroupEntity(bdbConsumerGroupEntity.getRecordKey());
                }
                this.consumerGroupTopicMap.remove(bdbEntity.getGroupTopicName());
                logger.info(strBuffer.append("[confDelTopicConfig  Success], ")
                        .append(bdbEntity.getRecordCreateUser()).append(" deleted all ")
                        .append(bdbEntity.getGroupTopicName()).append("'s records").toString());
            } else {
                BdbConsumerGroupEntity curEntity =
                        consumerGroupEntityMap.remove(bdbEntity.getConsumerGroupName());
                if (curEntity != null) {
                    mBdbStoreManagerService.delBdbConsumerGroupEntity(curEntity.getRecordKey());
                    strBuffer.append("[confDelTopicConfig  Success], ")
                            .append(bdbEntity.getRecordCreateUser()).append(" deleted record ");
                    logger.info(curEntity.toJsonString(strBuffer).toString());
                }
                if (consumerGroupEntityMap.isEmpty()) {
                    this.consumerGroupTopicMap.remove(bdbEntity.getGroupTopicName());
                }
            }
        } else {
            logger.info("[confDelTopicConfig  Success], not found record");
        }
        return true;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Add consumer group to blacklist
     *
     * @param bdbEntity the consumer group entity which will be delete
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confAddBdbBlackConsumerGroup(BdbBlackGroupEntity bdbEntity) throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        ConcurrentHashMap<String, BdbBlackGroupEntity> blackGroupEntityMap =
                blackGroupTopicMap.get(bdbEntity.getBlackGroupName());
        if (blackGroupEntityMap != null) {
            BdbBlackGroupEntity curEntity =
                    blackGroupEntityMap.get(bdbEntity.getTopicName());
            if (curEntity != null) {
                throw new Exception(strBuffer
                        .append("Duplicate add blackGroupName info, exist record is: ")
                        .append(curEntity).toString());
            }
        }
        boolean putResult =
                mBdbStoreManagerService.putBdbBlackGroupConfEntity(bdbEntity, true);
        if (putResult) {
            if (blackGroupEntityMap == null) {
                blackGroupEntityMap =
                        new ConcurrentHashMap<String, BdbBlackGroupEntity>();
                blackGroupTopicMap.put(bdbEntity.getBlackGroupName(), blackGroupEntityMap);
            }
            blackGroupEntityMap.put(bdbEntity.getTopicName(), bdbEntity);
            strBuffer.append("[confAddBlackConsumerGroup Success] ");
            logger.info(bdbEntity.toJsonString(strBuffer).toString());
            return true;
        }
        return false;
    }

    /**
     * Get black consumer group list via group entity
     *
     * @param bdbQueryEntity the consumer group entity
     * @return consumer group list
     * @throws Exception
     */
    public List<BdbBlackGroupEntity> confGetBdbBlackConsumerGroupSet(
            BdbBlackGroupEntity bdbQueryEntity) throws Exception {
        validMasterStatus();
        List<BdbBlackGroupEntity> bdbBlackGroupEntities = new ArrayList<BdbBlackGroupEntity>();
        for (ConcurrentHashMap<String, BdbBlackGroupEntity> blackGroupEntityMap
                : blackGroupTopicMap.values()) {
            for (BdbBlackGroupEntity entity : blackGroupEntityMap.values()) {
                if (bdbQueryEntity == null) {
                    bdbBlackGroupEntities.add(entity);
                    continue;
                }
                if ((!TStringUtils.isBlank(bdbQueryEntity.getBlackRecordKey())
                        && !bdbQueryEntity.getBlackRecordKey().equals(
                        entity.getBlackRecordKey()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getTopicName())
                        && !bdbQueryEntity.getTopicName()
                        .equals(entity.getTopicName()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getBlackGroupName())
                        && !bdbQueryEntity.getBlackGroupName().equals(entity.getBlackGroupName()))
                        || (!TStringUtils.isBlank(bdbQueryEntity.getCreateUser())
                        && !bdbQueryEntity.getCreateUser().equals(entity.getCreateUser()))) {
                    continue;
                }
                bdbBlackGroupEntities.add(entity);
            }
        }
        return bdbBlackGroupEntities;
    }

    /**
     * Delete black consumer group list from bdb
     *
     * @param bdbEntity the consumer group entity will be delete
     * @return true if success
     * @throws Exception
     */
    public boolean confDeleteBdbBlackConsumerGroupSet(BdbBlackGroupEntity bdbEntity)
            throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        ConcurrentHashMap<String, BdbBlackGroupEntity> blackGroupEntityMap =
                this.blackGroupTopicMap.get(bdbEntity.getBlackGroupName());
        if (blackGroupEntityMap != null) {
            if (TStringUtils.isBlank(bdbEntity.getTopicName())) {
                for (BdbBlackGroupEntity bdbBlackGroupEntity
                        : blackGroupEntityMap.values()) {
                    mBdbStoreManagerService
                            .delBdbBlackGroupEntity(bdbBlackGroupEntity.getBlackRecordKey());
                }
                this.blackGroupTopicMap.remove(bdbEntity.getBlackGroupName());
                logger.info(strBuffer.append("[confDelBlackGroup  Success], ")
                        .append(bdbEntity.getCreateUser())
                        .append(" deleted all ")
                        .append(bdbEntity.getBlackGroupName())
                        .append("'s records").toString());
            } else {
                BdbBlackGroupEntity curEntity =
                        blackGroupEntityMap.remove(bdbEntity.getTopicName());
                if (curEntity != null) {
                    mBdbStoreManagerService.delBdbBlackGroupEntity(curEntity.getBlackRecordKey());
                    strBuffer.append("[confDelBlackGroup  Success], ")
                            .append(bdbEntity.getCreateUser())
                            .append(" deleted record ");
                    logger.info(curEntity.toJsonString(strBuffer).toString());
                }
                if (blackGroupEntityMap.isEmpty()) {
                    this.blackGroupTopicMap.remove(bdbEntity.getBlackGroupName());
                }
            }
        } else {
            logger.info("[confDelBlackGroup  Success], not found record");
        }
        return true;
    }

    public List<String> getBdbBlackTopicList(String consumerGroupName) {
        ArrayList<String> blackTopicList = new ArrayList<String>();
        ConcurrentHashMap<String/* topicname */, BdbBlackGroupEntity> blackGroupEntityMap =
                this.blackGroupTopicMap.get(consumerGroupName);
        if (blackGroupEntityMap != null) {
            blackTopicList.addAll(blackGroupEntityMap.keySet());
        }
        return blackTopicList;
    }

    // /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Add consumer group setting
     *
     * @param bdbEntity the consumer group entity will be add
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confAddBdbConsumeGroupSetting(BdbConsumeGroupSettingEntity bdbEntity)
            throws Exception {
        validMasterStatus();
        BdbConsumeGroupSettingEntity curEntity =
                consumeGroupSettingMap.get(bdbEntity.getConsumeGroupName());
        if (curEntity != null) {
            throw new Exception(new StringBuilder(512)
                    .append("Duplicate add ConsumeGroupSetting info, exist record is: ")
                    .append(curEntity).toString());
        }
        boolean putResult =
                mBdbStoreManagerService.putBdbConsumeGroupSettingEntity(bdbEntity, true);
        if (putResult) {
            consumeGroupSettingMap.put(bdbEntity.getConsumeGroupName(), bdbEntity);
            logger.info(new StringBuilder(512)
                    .append("[confAddBdbConsumeGroupSetting Success] ")
                    .append(bdbEntity).toString());
            return true;
        }
        return false;
    }

    /**
     * Set consumer group setting
     *
     * @param bdbEntity the consumer group entity will be set
     * @return true if success otherwise false
     * @throws Exception
     */
    public boolean confUpdBdbConsumeGroupSetting(BdbConsumeGroupSettingEntity bdbEntity)
            throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        BdbConsumeGroupSettingEntity offsetResetGroupEntity =
                consumeGroupSettingMap.get(bdbEntity.getConsumeGroupName());
        if (offsetResetGroupEntity == null) {
            throw new Exception(strBuffer
                    .append("Update ConsumeGroupSetting failure, not exist record for groupName: ")
                    .append(bdbEntity.getConsumeGroupName()).toString());
        }
        boolean putResult =
                mBdbStoreManagerService.putBdbConsumeGroupSettingEntity(bdbEntity, false);
        if (putResult) {
            consumeGroupSettingMap.put(bdbEntity.getConsumeGroupName(), bdbEntity);
            strBuffer.append("[confUpdBdbConsumeGroupSetting Success] record from : ");
            strBuffer = offsetResetGroupEntity.toJsonString(strBuffer);
            strBuffer.append(" to : ");
            strBuffer = bdbEntity.toJsonString(strBuffer);
            logger.info(strBuffer.toString());
            return true;
        }
        return false;
    }

    public boolean confUpdBdbConsumeGroupLastUsedTime(String groupName) throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        BdbConsumeGroupSettingEntity offsetResetGroupEntity =
                consumeGroupSettingMap.get(groupName);
        if (offsetResetGroupEntity == null) {
            throw new Exception(strBuffer
                    .append("Update offsetResetGroup lastTime failure, not exist record for groupName: ")
                    .append(groupName).toString());
        }
        offsetResetGroupEntity.setLastUsedDateNow();
        boolean putResult =
                mBdbStoreManagerService
                        .putBdbConsumeGroupSettingEntity(offsetResetGroupEntity, false);
        if (putResult) {
            consumeGroupSettingMap
                    .put(offsetResetGroupEntity.getConsumeGroupName(), offsetResetGroupEntity);
            strBuffer.append("[confUpdBdbConsumeGroupLastUsedTime Success] cur record is ");
            logger.info(offsetResetGroupEntity.toJsonString(strBuffer).toString());
            return true;
        }
        return false;
    }

    /**
     * Get consumer group setting
     *
     * @param bdbQueryEntity
     * @return
     * @throws Exception
     */
    public List<BdbConsumeGroupSettingEntity> confGetBdbConsumeGroupSetting(
            BdbConsumeGroupSettingEntity bdbQueryEntity) throws Exception {
        validMasterStatus();
        List<BdbConsumeGroupSettingEntity> bdbOffsetRstGroupEntities =
                new ArrayList<BdbConsumeGroupSettingEntity>();
        for (BdbConsumeGroupSettingEntity entity
                : consumeGroupSettingMap.values()) {
            if (entity == null) {
                continue;
            }
            if ((!TStringUtils.isBlank(bdbQueryEntity.getConsumeGroupName())
                    && !bdbQueryEntity.getConsumeGroupName().equals(entity.getConsumeGroupName()))
                    || (bdbQueryEntity.getEnableBind() != TStatusConstants.STATUS_SERVICE_UNDEFINED
                    && bdbQueryEntity.getEnableBind() != entity.getEnableBind())
                    || (bdbQueryEntity.getAllowedBrokerClientRate() != TStatusConstants.STATUS_SERVICE_UNDEFINED
                    && bdbQueryEntity.getAllowedBrokerClientRate() != entity.getAllowedBrokerClientRate())
                    || (!TStringUtils.isBlank(bdbQueryEntity.getCreateUser())
                    && !bdbQueryEntity.getCreateUser().equals(entity.getCreateUser()))) {
                continue;
            }
            bdbOffsetRstGroupEntities.add(entity);
        }
        return bdbOffsetRstGroupEntities;
    }

    /**
     * Delete consumer group setting
     *
     * @param groupNameSet the group name set will be delete
     * @param strBuffer    the error info string buffer
     * @return true if success
     * @throws Exception
     */
    public boolean confDeleteBdbConsumeGroupSetting(Set<String> groupNameSet,
                                                    final StringBuilder strBuffer) throws Exception {
        validMasterStatus();
        for (String groupName : groupNameSet) {
            BdbConsumeGroupSettingEntity curEntity =
                    this.consumeGroupSettingMap.remove(groupName);
            if (curEntity != null) {
                mBdbStoreManagerService.delBdbConsumeGroupSettingEntity(groupName);
                strBuffer.append(
                        "[confDeleteBdbConsumeGroupSetting  Success], deleted consume group setting record :");
                logger.info(curEntity.toJsonString(strBuffer).toString());
                strBuffer.delete(0, strBuffer.length());
            } else {
                logger.info("[confDeleteBdbConsumeGroupSetting  Success], not found record");
            }
        }
        return true;
    }

    public BdbConsumeGroupSettingEntity getBdbConsumeGroupSetting(String consumeGroupName) {
        return this.consumeGroupSettingMap.get(consumeGroupName);
    }


    // ////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Add flow control
     *
     * @param bdbEntity
     * @return
     * @throws Exception
     */
    public boolean confAddBdbGroupFlowCtrl(BdbGroupFlowCtrlEntity bdbEntity) throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        BdbGroupFlowCtrlEntity curEntity =
                consumeGroupFlowCtrlMap.get(bdbEntity.getGroupName());
        if (curEntity != null) {
            throw new Exception(strBuffer
                    .append("Duplicate add groupFlowCtrl info, exist record is: ")
                    .append(curEntity).toString());
        }
        boolean putResult =
                this.mBdbStoreManagerService.putBdbGroupFlowCtrlConfEntity(bdbEntity, true);
        if (putResult) {
            this.consumeGroupFlowCtrlMap.put(bdbEntity.getGroupName(), bdbEntity);
            strBuffer.append("[confAddBdbGroupFlowCtrl Success] ");
            logger.info(bdbEntity.toJsonString(strBuffer).toString());
            return true;
        }
        return false;
    }

    /**
     * Update flow control
     *
     * @param bdbEntity
     * @return
     * @throws Exception
     */
    public boolean confUpdateBdbGroupFlowCtrl(BdbGroupFlowCtrlEntity bdbEntity) throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        BdbGroupFlowCtrlEntity curEntity =
                consumeGroupFlowCtrlMap.get(bdbEntity.getGroupName());
        if (curEntity == null) {
            throw new Exception(strBuffer.append("Not found ")
                    .append(bdbEntity.getGroupName())
                    .append("'groupFlowCtrl info!").toString());
        }
        this.mBdbStoreManagerService.putBdbGroupFlowCtrlConfEntity(bdbEntity, false);
        this.consumeGroupFlowCtrlMap.put(bdbEntity.getGroupName(), bdbEntity);
        strBuffer.append("[confUpdateBdbGroupFlowCtrl Success] ");
        logger.info(bdbEntity.toJsonString(strBuffer).toString());
        return true;
    }

    public List<BdbGroupFlowCtrlEntity> confGetBdbGroupFlowCtrl(
            BdbGroupFlowCtrlEntity bdbQueryEntity) throws Exception {
        validMasterStatus();
        List<BdbGroupFlowCtrlEntity> bdbGroupFlowCtrlEntities =
                new ArrayList<BdbGroupFlowCtrlEntity>();
        for (BdbGroupFlowCtrlEntity ctrlEntity : consumeGroupFlowCtrlMap.values()) {
            if (ctrlEntity == null) {
                continue;
            }
            if ((!TStringUtils.isBlank(bdbQueryEntity.getGroupName())
                    && !bdbQueryEntity.getGroupName().equals(ctrlEntity.getGroupName()))
                    || (!TStringUtils.isBlank(bdbQueryEntity.getCreateUser())
                    && !bdbQueryEntity.getCreateUser().equals(ctrlEntity.getCreateUser()))
                    || (bdbQueryEntity.getStatusId() != TBaseConstants.META_VALUE_UNDEFINED
                    && bdbQueryEntity.getStatusId() != ctrlEntity.getStatusId())
                    || (bdbQueryEntity.getQryPriorityId() != TBaseConstants.META_VALUE_UNDEFINED
                    && bdbQueryEntity.getQryPriorityId() != ctrlEntity.getQryPriorityId())) {
                continue;
            }
            bdbGroupFlowCtrlEntities.add(ctrlEntity);
        }
        return bdbGroupFlowCtrlEntities;
    }

    public BdbGroupFlowCtrlEntity getBdbGroupFlowCtrl(String consumerGroupName) {
        return this.consumeGroupFlowCtrlMap.get(consumerGroupName);
    }

    public BdbGroupFlowCtrlEntity getBdbDefFlowCtrl() {
        return this.consumeGroupFlowCtrlMap.get("default_master_ctrl");
    }

    /**
     * Delete flow control
     *
     * @param groupNameList
     * @return
     * @throws Exception
     */
    public boolean confDeleteBdbGroupFlowCtrl(Set<String> groupNameList) throws Exception {
        validMasterStatus();
        StringBuilder strBuffer = new StringBuilder(512);
        for (String consumerGroupName : groupNameList) {
            this.mBdbStoreManagerService.delBdbGroupFlowCtrlStoreEntity(consumerGroupName);
            BdbGroupFlowCtrlEntity curEntity =
                    consumeGroupFlowCtrlMap.remove(consumerGroupName);
            if (curEntity != null) {
                strBuffer.append("[confDeleteBdbGroupFlowCtrl  Success], removed record is ");
                logger.info(curEntity.toJsonString(strBuffer).toString());
                strBuffer.delete(0, strBuffer.length());
            } else {
                logger.info("[confDeleteBdbGroupFlowCtrl  Success], not found record");
            }
        }
        return true;
    }

    private void validMasterStatus() throws Exception {
        if (!isSelfMaster()) {
            throw new StandbyException("Please send your request to the master Node.");
        }
    }


}
