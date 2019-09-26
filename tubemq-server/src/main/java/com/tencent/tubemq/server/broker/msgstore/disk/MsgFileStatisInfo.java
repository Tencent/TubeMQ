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

package com.tencent.tubemq.server.broker.msgstore.disk;

import com.tencent.tubemq.server.common.TServerConstants;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/***
 * Statistics of message file. Contains read, write, failed, etc.
 */
public class MsgFileStatisInfo {
    private final FileStatisItemSet[] countSets = new FileStatisItemSet[2];
    private AtomicLong lastStatisTime = new AtomicLong(0);
    private AtomicBoolean isStart = new AtomicBoolean(false);
    private AtomicInteger index = new AtomicInteger(0);

    public MsgFileStatisInfo() {
        lastStatisTime.set(System.currentTimeMillis());
        countSets[0] = new FileStatisItemSet();
        countSets[1] = new FileStatisItemSet();
        for (int i = 0; i < 2; i++) {
            FileStatisItemSet curCountSet = countSets[i];
            curCountSet.startTime = System.currentTimeMillis();
            curCountSet.endTime = System.currentTimeMillis();
            curCountSet.msgTotalCnt.set(0);
            curCountSet.maxFlushMsgSize.set(0);
            curCountSet.minFlushMsgSize.set(Long.MAX_VALUE);
            curCountSet.dataFullCont.set(0);
            curCountSet.indexFullCont.set(0);
            curCountSet.countFullCont.set(0);
            curCountSet.timeFullCont.set(0);
            curCountSet.writeFailCont.set(0);
        }
        isStart.set(true);
    }

    public void addWriteFailCount() {
        if (isStart.get()) {
            FileStatisItemSet putCountSet = countSets[index.get()];
            if (putCountSet != null) {
                putCountSet.writeFailCont.incrementAndGet();
            }
            if (System.currentTimeMillis() - lastStatisTime.get()
                    > TServerConstants.CFG_STORE_STATS_MAX_REFRESH_DURATITON) {
                if (isStart.compareAndSet(true, false)) {
                    if (putCountSet != null) {
                        putCountSet.endTime = System.currentTimeMillis();
                    }

                }
            }
        }
    }

    public void addFullTypeCount(long currTime, boolean isDataFull, boolean isIndexFull,
                                 boolean isMsgCountFull, boolean isMsgTimeFull,
                                 long msgTotalSize, long msgTotalCnt) {
        if (isStart.get()) {
            FileStatisItemSet putCountSet = countSets[index.get()];
            if (putCountSet != null) {
                if (isDataFull) {
                    putCountSet.dataFullCont.incrementAndGet();
                }
                if (isIndexFull) {
                    putCountSet.indexFullCont.incrementAndGet();
                }
                if (isMsgCountFull) {
                    putCountSet.countFullCont.incrementAndGet();
                }
                if (isMsgTimeFull) {
                    putCountSet.timeFullCont.incrementAndGet();
                }
                if (msgTotalSize < putCountSet.minFlushMsgSize.get()) {
                    putCountSet.minFlushMsgSize.set(msgTotalSize);
                }
                if (msgTotalSize > putCountSet.maxFlushMsgSize.get()) {
                    putCountSet.maxFlushMsgSize.set(msgTotalSize);
                }
                putCountSet.msgTotalCnt.addAndGet(msgTotalCnt);
            }
            if (currTime - lastStatisTime.get()
                    > TServerConstants.CFG_STORE_STATS_MAX_REFRESH_DURATITON) {
                if (isStart.compareAndSet(true, false)) {
                    if (putCountSet != null) {
                        putCountSet.endTime = currTime;
                    }
                }
            }
        }
    }

    public String getCurMsgSizeStatisInfo(boolean needRefresh) {
        FileStatisItemSet oldCountSet;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8:00"));
        StringBuilder sBuilder = new StringBuilder(512);
        sBuilder.append("[");
        for (int item = 0; item < 2; item++) {
            oldCountSet = countSets[(index.get() + item) % 2];
            if (oldCountSet == null) {
                continue;
            }
            if (item > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"startTime\":\"").append(sdf.format(new Date(oldCountSet.startTime)))
                    .append("\",\"msgTotalCnt\":").append(oldCountSet.msgTotalCnt.get());
            sBuilder.append(",\"dataSegFullCont\":").append(oldCountSet.dataFullCont.get())
                    .append(",\"indexSegFullCont\":").append(oldCountSet.indexFullCont.get());
            sBuilder.append(",\"countFullCont\":").append(oldCountSet.countFullCont.get())
                    .append(",\"timeFullCont\":").append(oldCountSet.timeFullCont.get());
            sBuilder.append(",\"maxMsgTtlSize\":").append(oldCountSet.maxFlushMsgSize.get())
                    .append(",\"minMsgTtlSize\":").append(oldCountSet.minFlushMsgSize.get());
            sBuilder.append(",\"writeFailCont\":").append(oldCountSet.writeFailCont.get())
                    .append(",\"endTime\":\"").append(sdf.format(new Date(oldCountSet.endTime)))
                    .append("\"}");
        }
        sBuilder.append("]");
        if (needRefresh) {
            int curIndex = index.get();
            FileStatisItemSet curCountSet = countSets[(curIndex + 1) % 2];
            curCountSet.startTime = System.currentTimeMillis();
            curCountSet.endTime = System.currentTimeMillis();
            curCountSet.msgTotalCnt.set(0);
            curCountSet.maxFlushMsgSize.set(0);
            curCountSet.minFlushMsgSize.set(Long.MAX_VALUE);
            curCountSet.dataFullCont.set(0);
            curCountSet.indexFullCont.set(0);
            curCountSet.countFullCont.set(0);
            curCountSet.timeFullCont.set(0);
            curCountSet.writeFailCont.set(0);
            if (index.compareAndSet(curIndex, (curIndex + 1) % 2)) {
                isStart.compareAndSet(false, true);
                lastStatisTime.set(System.currentTimeMillis());
            }
        }
        return sBuilder.toString();
    }

    private static class FileStatisItemSet {
        public long startTime = System.currentTimeMillis();
        public AtomicLong msgTotalCnt = new AtomicLong(0);
        public AtomicLong maxFlushMsgSize = new AtomicLong(0);
        public AtomicLong minFlushMsgSize = new AtomicLong(Long.MAX_VALUE);
        public AtomicLong dataFullCont = new AtomicLong(0);
        public AtomicLong indexFullCont = new AtomicLong(0);
        public AtomicLong countFullCont = new AtomicLong(0);
        public AtomicLong timeFullCont = new AtomicLong(0);
        public AtomicLong writeFailCont = new AtomicLong(0);
        public long endTime = System.currentTimeMillis();
    }
}
