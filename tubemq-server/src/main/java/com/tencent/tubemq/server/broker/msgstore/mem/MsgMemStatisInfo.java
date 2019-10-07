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

import com.tencent.tubemq.server.common.TServerConstants;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/***
 * Statistics of message memory. Contains read, write, failed, etc. It's similar to MsgFileStatisInfo.
 */
public class MsgMemStatisInfo {
    private final MemStatisItemSet[] countSets = new MemStatisItemSet[2];
    private AtomicLong lastStatisTime = new AtomicLong(0);
    private AtomicBoolean isStart = new AtomicBoolean(false);
    private AtomicInteger index = new AtomicInteger(0);

    public MsgMemStatisInfo() {
        lastStatisTime.set(System.currentTimeMillis());
        countSets[0] = new MemStatisItemSet();
        countSets[1] = new MemStatisItemSet();
        isStart.set(true);
        for (int i = 0; i < 2; i++) {
            MemStatisItemSet curCountSet = countSets[i];
            curCountSet.startTime = System.currentTimeMillis();
            curCountSet.endTime = System.currentTimeMillis();
            curCountSet.msgTotalCnt.set(0);
            curCountSet.maxMsgSize.set(0);
            curCountSet.minMsgSize.set(Integer.MAX_VALUE);
            curCountSet.sizeDataFullCont.set(0);
            curCountSet.sizeIndexFullCont.set(0);
            curCountSet.countFullCont.set(0);
            curCountSet.memFlushCont.set(0);
            curCountSet.timeFullCont.set(0);
            curCountSet.writeFailCont.set(0);
            curCountSet.maxFlushTime.set(0);
        }
    }

    /***
     * Add write fail count statistic.
     */
    public void addWriteFailCount() {
        if (isStart.get()) {
            MemStatisItemSet putCountSet = countSets[index.get()];
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

    /***
     * Add full type count statistic.
     *
     * @param timeRecv
     * @param fullDataSize
     * @param fullIndexSize
     * @param fullCount
     */
    public void addFullTypeCount(long timeRecv, boolean fullDataSize,
                                 boolean fullIndexSize, boolean fullCount) {
        if (isStart.get()) {
            MemStatisItemSet putCountSet = countSets[index.get()];
            if (putCountSet != null) {
                if (fullDataSize) {
                    putCountSet.sizeDataFullCont.incrementAndGet();
                }
                if (fullIndexSize) {
                    putCountSet.sizeIndexFullCont.incrementAndGet();
                }
                if (fullCount) {
                    putCountSet.countFullCont.incrementAndGet();
                }
            }
            if (timeRecv - lastStatisTime.get()
                    > TServerConstants.CFG_STORE_STATS_MAX_REFRESH_DURATITON) {
                if (isStart.compareAndSet(true, false)) {
                    if (putCountSet != null) {
                        putCountSet.endTime = timeRecv;
                    }
                }
            }
        }
    }

    /***
     * Add memory flush count statistic.
     *
     * @param isTimeOut
     */
    public void addMemFlushCount(boolean isTimeOut) {
        if (isStart.get()) {
            MemStatisItemSet putCountSet = countSets[index.get()];
            if (putCountSet != null) {
                putCountSet.memFlushCont.incrementAndGet();
                if (isTimeOut) {
                    putCountSet.timeFullCont.incrementAndGet();
                }
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

    /***
     * Add flush time statistic.
     *
     * @param flushTIme
     */
    public void addFlushTimeStatis(long flushTIme) {
        if (isStart.get()) {
            MemStatisItemSet putCountSet = countSets[index.get()];
            if (putCountSet != null) {
                if (putCountSet.maxFlushTime.get() < flushTIme) {
                    putCountSet.maxFlushTime.set(flushTIme);
                }
            }
            if (flushTIme - lastStatisTime.get()
                    > TServerConstants.CFG_STORE_STATS_MAX_REFRESH_DURATITON) {
                if (isStart.compareAndSet(true, false)) {
                    if (putCountSet != null) {
                        putCountSet.endTime = flushTIme;
                    }
                }
            }
        }
    }

    /***
     * Add message size statistic.
     *
     * @param timeRecv
     * @param msgSize
     */
    public void addMsgSizeStatis(long timeRecv, int msgSize) {
        if (isStart.get()) {
            MemStatisItemSet putCountSet = countSets[index.get()];
            if (putCountSet != null) {
                putCountSet.msgTotalCnt.incrementAndGet();
                if (msgSize < putCountSet.minMsgSize.get()) {
                    putCountSet.minMsgSize.set(msgSize);
                }
                if (msgSize > putCountSet.maxMsgSize.get()) {
                    putCountSet.maxMsgSize.set(msgSize);
                }
            }
            if (timeRecv - lastStatisTime.get()
                    > TServerConstants.CFG_STORE_STATS_MAX_REFRESH_DURATITON) {
                if (isStart.compareAndSet(true, false)) {
                    if (putCountSet != null) {
                        putCountSet.endTime = timeRecv;
                    }
                }
            }
        }
    }

    /***
     * Get current message size status info.
     *
     * @param needRefresh
     * @return
     */
    public String getCurMsgSizeStatisInfo(boolean needRefresh) {
        MemStatisItemSet oldCountSet;
        StringBuilder sBuilder = new StringBuilder(512);
        sBuilder.append("[");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8:00"));
        for (int i = 0; i < 2; i++) {
            oldCountSet = countSets[(index.get() + i) % 2];
            if (oldCountSet == null) {
                continue;
            }
            if (i > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"startTime\":\"").append(sdf.format(new Date(oldCountSet.startTime)))
                    .append("\",\"msgTotalCnt\":").append(oldCountSet.msgTotalCnt.get());
            sBuilder.append(",\"maxMsgSize\":").append(oldCountSet.maxMsgSize.get())
                    .append(",\"minMsgSize\":").append(oldCountSet.minMsgSize.get());
            sBuilder.append(",\"memFlushCont\":").append(oldCountSet.memFlushCont.get())
                    .append(",\"sizeDataFullCont\":").append(oldCountSet.sizeDataFullCont.get());
            sBuilder.append(",\"sizeIndexFullCont\":").append(oldCountSet.sizeIndexFullCont.get())
                    .append(",\"countFullCont\":").append(oldCountSet.countFullCont.get());
            sBuilder.append(",\"timeFullCont\":").append(oldCountSet.timeFullCont.get())
                    .append(",\"writeFailCont\":").append(oldCountSet.writeFailCont.get());
            sBuilder.append(",\"maxFlushTime\":").append(oldCountSet.maxFlushTime.get())
                    .append(",\"endTime\":\"").append(sdf.format(new Date(oldCountSet.endTime)))
                    .append("\"}");
        }
        sBuilder.append("]");
        if (needRefresh) {
            int curIndex = index.get();
            MemStatisItemSet curCountSet = countSets[(curIndex + 1) % 2];
            curCountSet.startTime = System.currentTimeMillis();
            curCountSet.endTime = System.currentTimeMillis();
            curCountSet.msgTotalCnt.set(0);
            curCountSet.maxMsgSize.set(0);
            curCountSet.minMsgSize.set(Integer.MAX_VALUE);
            curCountSet.memFlushCont.set(0);
            curCountSet.sizeDataFullCont.set(0);
            curCountSet.sizeIndexFullCont.set(0);
            curCountSet.countFullCont.set(0);
            curCountSet.timeFullCont.set(0);
            curCountSet.writeFailCont.set(0);
            curCountSet.maxFlushTime.set(0);
            if (index.compareAndSet(curIndex, (curIndex + 1) % 2)) {
                isStart.compareAndSet(false, true);
                lastStatisTime.set(System.currentTimeMillis());
            }
        }
        return sBuilder.toString();
    }

    private static class MemStatisItemSet {
        public long startTime = System.currentTimeMillis();
        public AtomicLong msgTotalCnt = new AtomicLong(0);
        public AtomicInteger maxMsgSize = new AtomicInteger(0);
        public AtomicInteger minMsgSize = new AtomicInteger(Integer.MAX_VALUE);
        public AtomicLong memFlushCont = new AtomicLong(0);
        public AtomicLong sizeDataFullCont = new AtomicLong(0);
        public AtomicLong sizeIndexFullCont = new AtomicLong(0);
        public AtomicLong countFullCont = new AtomicLong(0);
        public AtomicLong timeFullCont = new AtomicLong(0);
        public AtomicLong writeFailCont = new AtomicLong(0);
        public AtomicLong maxFlushTime = new AtomicLong(0);
        public long endTime = System.currentTimeMillis();
    }
}
