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

package com.tencent.tubemq.example;

import com.tencent.tubemq.corebase.utils.ThreadUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to collect and report message received statistics.
 */
public class MsgRecvStats implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MsgRecvStats.class);
    private static final ConcurrentHashMap<String, AtomicLong> counterMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, AtomicLong> befCountMap = new ConcurrentHashMap<>();
    private AtomicBoolean isStarted = new AtomicBoolean(false);

    @Override
    public void run() {
        while (isStarted.get()) {
            try {
                for (Map.Entry<String, AtomicLong> entry : counterMap.entrySet()) {
                    long currCount = entry.getValue().get();

                    AtomicLong befCount = befCountMap.get(entry.getKey());
                    if (befCount == null) {
                        AtomicLong tmpCount = new AtomicLong(0);
                        befCount = befCountMap.putIfAbsent(entry.getKey(), tmpCount);
                        if (befCount == null) {
                            befCount = tmpCount;
                        }
                    }

                    logger.info("********* Current {} Message receive count is {}, dlt is {}",
                        new Object[]{entry.getKey(), currCount, (currCount - befCount.get())});

                }
            } catch (Throwable t) {
                // ignore
            }
            ThreadUtils.sleep(30000);
        }
    }

    public void addMsgCount(final String topicName, int msgCnt) {
        if (msgCnt > 0) {
            AtomicLong currCount = counterMap.get(topicName);
            if (currCount == null) {
                AtomicLong tmpCount = new AtomicLong(0);
                currCount = counterMap.putIfAbsent(topicName, tmpCount);
                if (currCount == null) {
                    currCount = tmpCount;
                }
            }

            if (currCount.addAndGet(msgCnt) % 500 == 0) {
                logger.info("Receive messages:" + currCount.get());
            }
        }
    }

    public void stopStats() {
        isStarted.set(false);
    }
}
