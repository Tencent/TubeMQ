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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.springframework.util.Assert;

/***
 * BrokerMetadataManage test
 */
public class BrokerMetadataManageTest {

    // brokerMetadataManage
    BrokerMetadataManage brokerMetadataManage;

    @Test
    public void updateBrokerTopicConfigMap() {
        brokerMetadataManage = new BrokerMetadataManage();
        // topic default config
        String newBrokerDefMetaConfInfo = "1:true:true:1000:10000:0,0,6:delete,168h:1:1000:1024:1000:1000";
        List<String> newTopicMetaConfInfoList = new LinkedList<>();
        // add topic custom config.
        newTopicMetaConfInfoList.add("topic1:2:true:true:1000:10000:0,0,6:delete,168h:1:1000:1024:1000:1000:1");
        newTopicMetaConfInfoList.add("topic2:4:true:true:1000:10000:0,0,6:delete,168h:1:1000:1024:1000:1000:1");
        brokerMetadataManage.updateBrokerTopicConfigMap(0L, 0,
                newBrokerDefMetaConfInfo, newTopicMetaConfInfoList, true, new StringBuilder());
        // get topic custom config.
        long count = brokerMetadataManage.getNumPartitions("topic2");
        Assert.isTrue(count == 4);
        // add topic custom config.
        newTopicMetaConfInfoList.add("topic3:6:true:true:1000:10000:0,0,6:delete,168h:1:1000:1024:1000:1000:1");
        brokerMetadataManage.updateBrokerTopicConfigMap(0L, 1,
                newBrokerDefMetaConfInfo, newTopicMetaConfInfoList, true, new StringBuilder());
        count = brokerMetadataManage.getNumPartitions("topic3");
        Assert.isTrue(count == 6);
    }

    @Test
    public void updateBrokerRemoveTopicMap() {
        brokerMetadataManage = new BrokerMetadataManage();
        String newBrokerDefMetaConfInfo = "1:true:true:1000:10000:0,0,6:delete,168h:1:1000:1024:1000:1000";
        List<String> newTopicMetaConfInfoList = new LinkedList<>();
        // add topic custom config.
        newTopicMetaConfInfoList.add("topic1:2:true:true:1000:10000:0,0,6:delete,168h:1:1000:1024:1000:1000:1");
        newTopicMetaConfInfoList.add("topic2:4:true:true:1000:10000:0,0,6:delete,168h:1:1000:1024:1000:1000:1");
        brokerMetadataManage.updateBrokerTopicConfigMap(0L, 0,
                newBrokerDefMetaConfInfo, newTopicMetaConfInfoList, true, new StringBuilder());
        Map<String, TopicMetadata> topicMetadataMap = brokerMetadataManage.getRemovedTopicConfigMap();
        Assert.isTrue(topicMetadataMap.size() == 0);
        List<String> rmvTopics = new LinkedList<>();
        rmvTopics.add("topic2:4:true:true:1000:10000:0,0,6:delete,168h:1:1000:1024:1000:1000:1");
        // update topic custom config.
        brokerMetadataManage.updateBrokerRemoveTopicMap(true, rmvTopics, new StringBuilder());
        topicMetadataMap = brokerMetadataManage.getRemovedTopicConfigMap();
        Assert.isTrue(topicMetadataMap.size() == 1);
    }
}
