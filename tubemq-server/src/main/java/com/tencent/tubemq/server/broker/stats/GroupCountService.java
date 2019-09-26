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

package com.tencent.tubemq.server.broker.stats;

import com.tencent.tubemq.corebase.daemon.AbstractDaemonService;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Statistics of broker. It use two CountSet alternatively print statistics to log.
 */
public class GroupCountService extends AbstractDaemonService implements CountService {
    private final Logger logger;
    private final String cntHdr;
    private final CountSet[] countSets = new CountSet[2];
    private AtomicInteger index = new AtomicInteger(0);


    public GroupCountService(String logFileName, String countType, long scanIntervalMs) {
        super(logFileName, scanIntervalMs);
        this.cntHdr = countType;
        if (logFileName == null) {
            this.logger = LoggerFactory.getLogger(GroupCountService.class);
        } else {
            this.logger = LoggerFactory.getLogger(logFileName);
        }
        countSets[0] = new CountSet();
        countSets[1] = new CountSet();
        super.start();
    }

    @Override
    protected void loopProcess(long intervalMs) {
        int tmpIndex = 0;
        int befIndex = 0;
        AtomicLong curRunCnt;
        ConcurrentHashMap<String, CountItem> counters;
        while (!super.isStopped()) {
            try {
                Thread.sleep(intervalMs);
                befIndex = tmpIndex = index.get();
                if (index.compareAndSet(befIndex, (++tmpIndex) % 2)) {
                    curRunCnt = countSets[befIndex].refCnt;
                    do {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            return;
                        }
                    } while (curRunCnt.get() > 0);
                    counters = countSets[befIndex].counterItem;
                    if (counters != null) {
                        for (Map.Entry<String, CountItem> entry : counters.entrySet()) {
                            logger.info("{}#{}#{}#{}", new Object[]{cntHdr, entry.getKey(),
                                    entry.getValue().getMsgCount(), entry.getValue().getMsgSize()});
                        }
                        counters.clear();
                    }
                }
            } catch (InterruptedException e) {
                return;
            } catch (Throwable t) {
                //
            }
        }
    }

    @Override
    public void close(long waitTimeMs) {
        if (super.stop()) {
            return;
        }
        int befIndex = index.get();
        ConcurrentHashMap<String, CountItem> counters;
        for (int i = 0; i < countSets.length; i++) {
            counters = countSets[(++befIndex) % 2].counterItem;
            if (counters != null) {
                for (Map.Entry<String, CountItem> entry : counters.entrySet()) {
                    logger.info("{}#{}#{}#{}", new Object[]{cntHdr, entry.getKey(),
                            entry.getValue().getMsgCount(), entry.getValue().getMsgSize()});
                }
                counters.clear();
            }
        }
    }

    @Override
    public void add(Map<String, CountItem> counterGroup) {
        CountSet countSet = countSets[index.get()];
        countSet.refCnt.incrementAndGet();
        ConcurrentHashMap<String, CountItem> counters = countSet.counterItem;
        for (Entry<String, CountItem> entry : counterGroup.entrySet()) {
            CountItem currData = counters.get(entry.getKey());
            if (currData == null) {
                CountItem tmpData = new CountItem(0L, 0L);
                currData = counters.putIfAbsent(entry.getKey(), tmpData);
                if (currData == null) {
                    currData = tmpData;
                }
            }
            currData.appendMsg(entry.getValue().getMsgCount(), entry.getValue().getMsgSize());
        }
        countSet.refCnt.decrementAndGet();
    }

    @Override
    public void add(String name, Long delta, int msgSize) {
        CountSet countSet = countSets[index.get()];
        countSet.refCnt.incrementAndGet();
        CountItem currData = countSet.counterItem.get(name);
        if (currData == null) {
            CountItem tmpData = new CountItem(0L, 0L);
            currData = countSet.counterItem.putIfAbsent(name, tmpData);
            if (currData == null) {
                currData = tmpData;
            }
        }
        currData.appendMsg(delta, msgSize);
        countSet.refCnt.decrementAndGet();
    }

    private class CountSet {
        public AtomicLong refCnt = new AtomicLong(0);
        public ConcurrentHashMap<String, CountItem> counterItem =
                new ConcurrentHashMap<String, CountItem>();
    }
}
