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

package com.tencent.tubemq.client.factory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.corebase.cluster.MasterInfo;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AddressUtils.class)
public class TubeSingleSessionFactoryTest {
    @Test
    public void testTubeSingleSessionFactory() throws Exception {
        TubeClientConfig config = mock(TubeClientConfig.class);
        when(config.getRpcConnProcesserCnt()).thenReturn(1);
        when(config.getRpcRspCallBackThreadCnt()).thenReturn(1);
        when(config.getMasterInfo()).thenReturn(new MasterInfo("192.168.1.1:18080"));

        PowerMockito.mockStatic(AddressUtils.class);
        PowerMockito.when(AddressUtils.getLocalAddress()).thenReturn("127.0.0.1");
        TubeSingleSessionFactory factory = new TubeSingleSessionFactory(config);
        factory.shutdown();
    }
}
