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


import com.tencent.tubemq.corebase.utils.ConcurrentHashSet;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TopicPSInfoManagerTest {
    private TopicPSInfoManager topicPSInfoManager;

    @Before
    public void setUp() throws Exception {
        topicPSInfoManager = new TopicPSInfoManager();
    }

    @After
    public void tearDown() throws Exception {
        topicPSInfoManager.clear();
    }

    @Test
    public void topicSubInfo() {
        ConcurrentHashSet<String> groupSet = new ConcurrentHashSet<>();
        groupSet.add("group_001");
        groupSet.add("group_002");
        groupSet.add("group_003");

        topicPSInfoManager.setTopicSubInfo("topic001", groupSet);
        ConcurrentHashSet<String> gs1 = topicPSInfoManager.getTopicSubInfo("topic001");
        Assert.assertEquals(3, gs1.size());

        topicPSInfoManager.removeTopicSubInfo("topic001", "group_001");
        topicPSInfoManager.removeTopicSubInfo("topic001", "group_002");
        gs1 = topicPSInfoManager.getTopicSubInfo("topic001");
        Assert.assertEquals(1, gs1.size());
    }

    @Test
    public void topicPubInfo() {
        HashSet<String> topicList = new HashSet<>();
        topicList.add("topic001");
        topicList.add("topic002");
        topicList.add("topic003");

        topicPSInfoManager.addProducerTopicPubInfo("producer_001", topicList);
        ConcurrentHashSet<String> ti1 = topicPSInfoManager.getTopicPubInfo("topic001");
        Assert.assertEquals(1, ti1.size());
        Assert.assertTrue(ti1.contains("producer_001"));

        topicPSInfoManager.rmvProducerTopicPubInfo("producer_001",
                new HashSet<String>(Arrays.asList("topic001", "topic002")));

        ConcurrentHashSet<String> ti2 = topicPSInfoManager.getTopicPubInfo("topic003");
        Assert.assertEquals(1, ti2.size());
        Assert.assertTrue(ti2.contains("producer_001"));
    }
}
