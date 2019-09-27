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

import static org.mockito.Mockito.mock;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BrokerInfoHolderTest {
    private BrokerInfoHolder brokerInfoHolder;
    private BrokerConfManage brokerConfManage;

    @Before
    public void setUp() throws Exception {
        brokerConfManage = mock(BrokerConfManage.class);
        brokerInfoHolder = new BrokerInfoHolder(10, brokerConfManage);
    }

    @Test
    public void testBrokerInfo() {
        BrokerInfo brokerInfo = mock(BrokerInfo.class);

        brokerInfoHolder.setBrokerInfo(1, brokerInfo);
        Assert.assertEquals(brokerInfo, brokerInfoHolder.getBrokerInfo(1));

        brokerInfoHolder.removeBroker(1);
        Assert.assertNull(brokerInfoHolder.getBrokerInfo(1));
    }
}
