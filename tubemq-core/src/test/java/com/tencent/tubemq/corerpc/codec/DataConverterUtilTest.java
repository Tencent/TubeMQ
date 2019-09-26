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

package com.tencent.tubemq.corerpc.codec;

import static org.junit.Assert.assertEquals;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.cluster.SubscribeInfo;
import com.tencent.tubemq.corebase.cluster.TopicInfo;
import com.tencent.tubemq.corebase.utils.DataConverterUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class DataConverterUtilTest {

    private static boolean subscribeInfoEqual(SubscribeInfo o1, SubscribeInfo o2) {
        return o1.getPartition().equals(o2.getPartition())
                && o1.getConsumerId().equals(o2.getConsumerId())
                && o1.getGroup().equals(o2.getGroup());
    }

    @Test
    public void testDataConvert() {
        // broker convert
        BrokerInfo broker = new BrokerInfo(0, "localhost", 1200);
        List<String> strInfoList = new ArrayList<String>();
        strInfoList.add("0:localhost:1200");
        Map<Integer, BrokerInfo> brokerMap = DataConverterUtil.convertBrokerInfo(strInfoList);
        assertEquals("broker should be equal", broker, brokerMap.get(broker.getBrokerId()));

        // partition convert
        Partition partition = new Partition(broker, "tube", 0);
        strInfoList.clear();
        strInfoList.add("0:localhost:1200#tube:0");
        List<Partition> partitionList = DataConverterUtil.convertPartitionInfo(strInfoList);
        assertEquals("partition should be equal", partition, partitionList.get(0));

        // SubscribeInfo convert
        SubscribeInfo subscribeInfo = new SubscribeInfo("001", "group", partition);
        strInfoList.clear();
        strInfoList.add("001@group#0:localhost:1200#tube:0");
        List<SubscribeInfo> subscribeInfoList = DataConverterUtil.convertSubInfo(strInfoList);
        assertEquals("subscribe should be equal", subscribeInfoEqual(subscribeInfo, subscribeInfoList.get(0)), true);

        // topic convert
        TopicInfo topic = new TopicInfo(broker, "tube", 10, 5, true, true);
        strInfoList.clear();
        // topic#brokerId:partitionNum:topicStoreNum
        strInfoList.add("tube#0:10:5");
        List<TopicInfo> topicList = DataConverterUtil.convertTopicInfo(brokerMap, strInfoList);
        assertEquals("topic should be equal", topic, topicList.get(0));

    }

}
