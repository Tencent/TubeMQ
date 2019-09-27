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

package com.tencent.tubemq.server.broker.msgstore.mem;

import java.nio.ByteBuffer;
import org.junit.Test;

/***
 * MsgMemStore test.
 */
public class MsgMemStoreTest {

    @Test
    public void appendMsg() {
        int maxCacheSize = 2 * 1024 * 1024;
        int maxMsgCount = 10000;
        MsgMemStore msgMemStore = new MsgMemStore(maxCacheSize, maxMsgCount, null);
        MsgMemStatisInfo msgMemStatisInfo = new MsgMemStatisInfo();
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put("abc".getBytes());
        // append data
        msgMemStore.appendMsg(msgMemStatisInfo, 0, 0, System.currentTimeMillis(), 3, bf);
    }

    @Test
    public void getMessages() {
        int maxCacheSize = 2 * 1024 * 1024;
        int maxMsgCount = 10000;
        MsgMemStore msgMemStore = new MsgMemStore(maxCacheSize, maxMsgCount, null);
        MsgMemStatisInfo msgMemStatisInfo = new MsgMemStatisInfo();
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put("abc".getBytes());
        msgMemStore.appendMsg(msgMemStatisInfo, 0, 0, System.currentTimeMillis(), 3, bf);
        // get messages
        GetCacheMsgResult getCacheMsgResult = msgMemStore.getMessages(0, 2, 1024, 1000, 0, false, false, null);
    }
}
