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

package com.tencent.tubemq.server.broker.utils;

import java.nio.ByteBuffer;
import org.junit.Test;
import org.springframework.util.Assert;

/***
 * DataStoreUtils test.
 */
public class DataStoreUtilsTest {

    @Test
    public void getInt() {
        ByteBuffer bf = ByteBuffer.allocate(100);
        bf.putInt(123);
        byte[] data = bf.array();
        int offset = 0;
        int val = DataStoreUtils.getInt(offset, data);
        // get int by DataStoreUtils
        Assert.isTrue(val == 123);
    }
}
