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

package com.tencent.tubemq.server.master.bdbstore;

import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBlackGroupEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumeGroupSettingEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumerGroupEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicAuthControlEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;


public interface BdbStoreService {

    void cleanData();

    boolean isMaster();

    long getMasterStartTime();

    InetSocketAddress getMasterAddress();

    ConcurrentHashMap<String/* nodeName */, MasterNodeInfo> getMasterGroupNodeInfo();

    boolean putBdbBrokerConfEntity(BdbBrokerConfEntity bdbBrokerConfEntity, boolean isNew);

    boolean delBdbBrokerConfEntity(int brokerId);

    ConcurrentHashMap<Integer/* brokerId */, BdbBrokerConfEntity> getBrokerConfigMap();

    boolean putBdbTopicConfEntity(BdbTopicConfEntity bdbTopicConfEntity, boolean isNew);

    boolean delBdbTopicConfEntity(String recordKey, String topicName);

    ConcurrentHashMap<Integer/* brokerId */,
            ConcurrentHashMap<String/* topicName */, BdbTopicConfEntity>> getBrokerTopicEntityMap();

    boolean putBdbTopicAuthControlEntity(BdbTopicAuthControlEntity bdbTopicAuthControlEntity,
                                         boolean isNew);

    boolean delBdbTopicAuthControlEntity(String topicName);

    ConcurrentHashMap<String, BdbTopicAuthControlEntity> getTopicAuthControlMap();

    boolean putBdbConsumerGroupConfEntity(BdbConsumerGroupEntity bdbConsumerGroupEntity,
                                          boolean isNew);

    boolean delBdbConsumerGroupEntity(String recordKey);

    ConcurrentHashMap<String,
            ConcurrentHashMap<String, BdbConsumerGroupEntity>> getConsumerGroupNameAccControlMap();

    MasterGroupStatus getMasterGroupStatus(boolean isFromHeartbeat);

    boolean isPrimaryNodeActived();

    void transferMaster() throws Exception;

    boolean putBdbBlackGroupConfEntity(BdbBlackGroupEntity bdbBlackGroupEntity, boolean isNew);

    boolean delBdbBlackGroupEntity(String recordKey);

    ConcurrentHashMap<String,
            ConcurrentHashMap<String, BdbBlackGroupEntity>> getBlackGroupNameAccControlMap();

    boolean delBdbGroupFilterCondEntity(String recordKey);

    boolean putBdbGroupFilterCondConfEntity(BdbGroupFilterCondEntity bdbGroupFilterCondEntity,
                                            boolean isNew);

    ConcurrentHashMap<String,
            ConcurrentHashMap<String, BdbGroupFilterCondEntity>> getGroupFilterCondAccControlMap();

    boolean delBdbGroupFlowCtrlStoreEntity(String groupName);

    boolean putBdbGroupFlowCtrlConfEntity(BdbGroupFlowCtrlEntity bdbGroupFlowCtrlEntity,
                                          boolean isNew);

    ConcurrentHashMap<String, BdbGroupFlowCtrlEntity> getGroupFlowCtrlMap();

    boolean delBdbConsumeGroupSettingEntity(String consumeGroupName);

    boolean putBdbConsumeGroupSettingEntity(BdbConsumeGroupSettingEntity offsetResetGroupEntity,
                                            boolean isNew);

    ConcurrentHashMap<String, BdbConsumeGroupSettingEntity> getConsumeGroupSettingMap();
}
