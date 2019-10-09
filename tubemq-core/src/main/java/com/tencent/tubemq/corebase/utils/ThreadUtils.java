/*
 * Tencent is pleased to support the open source community by making TubeMQ available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.corebase.utils;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread Utility
 * Copied from <a href="http://hbase.apache.org">Apache HBase Project</a>
 */
public class ThreadUtils {
    private static final Logger logger = LoggerFactory.getLogger(ThreadUtils.class);
    private static final AtomicInteger poolNumber = new AtomicInteger(1);

    /**
     * Utility method that sets name, daemon status and starts passed thread.
     *
     * @param t thread to run
     * @return Returns the passed Thread <code>t</code>.
     */
    public static Thread setDaemonThreadRunning(final Thread t) {
        return setDaemonThreadRunning(t, t.getName());
    }

    /**
     * Utility method that sets name, daemon status and starts passed thread.
     *
     * @param t    thread to frob
     * @param name new name
     * @return Returns the passed Thread <code>t</code>.
     */
    public static Thread setDaemonThreadRunning(final Thread t, final String name) {
        return setDaemonThreadRunning(t, name, null);
    }

    /**
     * Utility method that sets name, daemon status and starts passed thread.
     *
     * @param t       thread to frob
     * @param name    new name
     * @param handler A handler to set on the thread. Pass null if want to use default handler.
     * @return Returns the passed Thread <code>t</code>.
     */
    public static Thread setDaemonThreadRunning(final Thread t, final String name,
                                                final UncaughtExceptionHandler handler) {
        t.setName(name);
        if (handler != null) {
            t.setUncaughtExceptionHandler(handler);
        }
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Shutdown passed thread using isAlive and join.
     *
     * @param t Thread to shutdown
     */
    public static void shutdown(final Thread t) {
        shutdown(t, 0);
    }

    /**
     * Shutdown passed thread using isAlive and join.
     *
     * @param joinwait Pass 0 if we're to wait forever.
     * @param t        Thread to shutdown
     */
    public static void shutdown(final Thread t, final long joinwait) {
        if (t == null) {
            return;
        }
        while (t.isAlive()) {
            try {
                t.join(joinwait);
            } catch (InterruptedException e) {
                logger.warn(t.getName() + "; joinwait=" + joinwait, e);
            }
        }
    }

    /**
     * @param t Waits on the passed thread to die dumping a threaddump every minute while its up.
     */
    public static void threadDumpingIsAlive(final Thread t) throws InterruptedException {
        if (t == null) {
            return;
        }

        while (t.isAlive()) {
            t.join(60 * 1000);
            if (t.isAlive()) {
                logger.info("Automatic Stack Trace every 60 seconds waiting on " + t.getName());
            }
        }
    }

    /**
     * @param millis How long to sleep for in milliseconds.
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
