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

package com.tencent.tubemq.client.producer;

import static org.junit.Assert.assertEquals;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class RoundRobinPartitionRouterTest {

    @Test(expected = TubeClientException.class)
    public void testGetPartitionInvalidInput() throws TubeClientException {
        RoundRobinPartitionRouter router = new RoundRobinPartitionRouter();
        Message message = new Message("test", new byte[]{1, 2, 3});
        List<Partition> partitions = new ArrayList<Partition>();
        router.getPartition(message, partitions);
    }

    @Test
    public void testGetPartition() throws TubeClientException {
        RoundRobinPartitionRouter router = new RoundRobinPartitionRouter();
        Message message = new Message("test", new byte[]{1, 2, 3});
        List<Partition> partitions = new ArrayList<Partition>();
        Partition partition = new Partition(new BrokerInfo("0:127.0.0.1:18080"), "test", 0);
        partitions.add(partition);
        Partition actualPartition = router.getPartition(message, partitions);
        assertEquals(0, actualPartition.getPartitionId());

        partition.setDelayTimeStamp(System.currentTimeMillis() + 10000000);
        actualPartition = router.getPartition(message, partitions);
        assertEquals(0, actualPartition.getPartitionId());
    }
}
