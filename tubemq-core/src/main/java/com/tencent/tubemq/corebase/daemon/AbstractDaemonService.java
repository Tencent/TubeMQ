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

package com.tencent.tubemq.corebase.daemon;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractDaemonService implements Service, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDaemonService.class);
    private final String name;
    private final long intervalMs;
    private final Thread daemon;
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public AbstractDaemonService(final String serviceName, final long intervalMs) {
        this.name = serviceName;
        this.intervalMs = intervalMs;
        this.daemon = new Thread(this, serviceName + "-daemon-thread");
        this.daemon.setDaemon(true);
    }

    @Override
    public void run() {
        logger.info(new StringBuilder(256).append(name)
                .append("-daemon-thread started").toString());
        this.loopProcess(this.intervalMs);
        logger.info(new StringBuilder(256).append(name)
                .append("-daemon-thread stopped").toString());
    }

    protected abstract void loopProcess(long intervalMs);

    @Override
    public void start() {
        this.daemon.start();
    }

    @Override
    public boolean isStopped() {
        return this.shutdown.get();
    }

    @Override
    public boolean stop() {
        if (this.shutdown.get()) {
            return true;
        }
        if (this.shutdown.compareAndSet(false, true)) {
            logger.info(new StringBuilder(256).append(name)
                    .append("-daemon-thread closing ......").toString());
            try {
                if (this.daemon != null) {
                    this.daemon.interrupt();
                    this.daemon.join();
                }
            } catch (Throwable e) {
                //
            }
            logger.info(new StringBuilder(256).append(name)
                    .append("-daemon-thread stopped!").toString());
            return false;
        }
        return true;
    }
}
