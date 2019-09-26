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

package com.tencent.tubemq.corebase.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServiceStatusHolder {
    private static final Logger logger =
            LoggerFactory.getLogger(ServiceStatusHolder.class);
    private static AtomicBoolean isServiceStopped = new AtomicBoolean(false);
    private static AtomicBoolean isReadStopped = new AtomicBoolean(false);
    private static AtomicBoolean isWriteStopped = new AtomicBoolean(false);

    private static int allowedReadIOExcptCnt = 10;
    private static int allowedWriteIOExcptCnt = 10;
    private static long statisDurationMs = 120000;
    private static AtomicLong curReadIOExcptCnt = new AtomicLong(0);
    private static AtomicLong lastReadStatsTime =
            new AtomicLong(System.currentTimeMillis());
    private static AtomicBoolean isPauseRead = new AtomicBoolean(false);

    private static AtomicLong curWriteIOExcptCnt = new AtomicLong(0);
    private static AtomicLong lastWriteStatsTime =
            new AtomicLong(System.currentTimeMillis());
    private static AtomicBoolean isPauseWrite = new AtomicBoolean(false);


    public static void setStatisParameters(int paraAllowedReadIOExcptCnt,
                                           int paraAllowedWriteIOExcptCnt,
                                           long paraStatisDurationMs) {
        allowedReadIOExcptCnt = paraAllowedReadIOExcptCnt;
        allowedWriteIOExcptCnt = paraAllowedWriteIOExcptCnt;
        statisDurationMs = paraStatisDurationMs;
    }

    public static boolean isServiceStopped() {
        return isServiceStopped.get();
    }

    public static void setStoppedStatus(boolean isStopped, String caller) {
        if (isServiceStopped.get() != isStopped) {
            isServiceStopped.set(isStopped);
            if (isStopped) {
                logger.warn(new StringBuilder(256)
                        .append("[Service Status]: global-write stopped by caller ")
                        .append(caller).toString());
            }
        }
    }

    public static boolean addWriteIOErrCnt() {
        long curTime = lastWriteStatsTime.get();
        if (System.currentTimeMillis() - curTime > statisDurationMs) {
            if (lastWriteStatsTime.compareAndSet(curTime, System.currentTimeMillis())) {
                curWriteIOExcptCnt.getAndSet(0);
                if (isPauseWrite.get()) {
                    isPauseWrite.compareAndSet(true, false);
                }
            }
        }
        if (curWriteIOExcptCnt.incrementAndGet() > allowedWriteIOExcptCnt) {
            isPauseWrite.set(true);
            return true;
        }
        return false;
    }

    public static boolean isWriteServiceStop() {
        return isPauseWrite.get() || isWriteStopped.get();
    }

    public static int getWriteServiceReportStatus() {
        return getCurServiceStatus(isPauseWrite.get(), isWriteStopped.get());
    }

    public static boolean addReadIOErrCnt() {
        long curTime = lastReadStatsTime.get();
        if (System.currentTimeMillis() - curTime > statisDurationMs) {
            if (lastReadStatsTime.compareAndSet(curTime, System.currentTimeMillis())) {
                curReadIOExcptCnt.getAndSet(0);
                if (isPauseRead.get()) {
                    isPauseRead.compareAndSet(true, false);
                }
            }
        }
        if (curReadIOExcptCnt.incrementAndGet() > allowedReadIOExcptCnt) {
            isPauseRead.set(true);
            return true;
        }
        return false;
    }

    public static boolean isReadServiceStop() {
        return isPauseRead.get() || isReadStopped.get();
    }

    public static int getReadServiceReportStatus() {
        return getCurServiceStatus(isPauseRead.get(), isReadStopped.get());
    }

    public static void setReadWriteServiceStatus(boolean isReadStop,
                                                 boolean isWriteStop,
                                                 String caller) {
        if (isReadStopped.get() != isReadStop) {
            isReadStopped.set(isReadStop);
            if (isReadStop) {
                logger.warn(new StringBuilder(256)
                        .append("[Service Status]: global-read stopped by caller ")
                        .append(caller).toString());
            } else {
                if (isPauseRead.get()) {
                    isPauseRead.set(false);
                    curReadIOExcptCnt.set(0);
                    lastReadStatsTime.set(System.currentTimeMillis());
                }
                logger.warn(new StringBuilder(256)
                        .append("[Service Status]: global-read opened by caller ")
                        .append(caller).toString());
            }
        }
        if (isWriteStopped.get() != isWriteStop) {
            isWriteStopped.set(isWriteStop);
            if (isWriteStop) {
                logger.warn(new StringBuilder(256)
                        .append("[Service Status]: global-write stopped by caller ")
                        .append(caller).toString());
            } else {
                if (isPauseWrite.get()) {
                    isPauseWrite.set(false);
                    curWriteIOExcptCnt.set(0);
                    lastWriteStatsTime.set(System.currentTimeMillis());
                }
                logger.warn(new StringBuilder(256)
                        .append("[Service Status]: global-write opened by caller ")
                        .append(caller).toString());
            }
        }
    }

    private static int getCurServiceStatus(boolean pauseStatus, boolean serviceStatus) {
        if (serviceStatus) {
            return 1;
        } else {
            if (pauseStatus) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
