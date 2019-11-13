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

package com.tencent.tubemq.server.master.utils;

import com.tencent.tubemq.corebase.daemon.AbstractDaemonService;
import com.tencent.tubemq.server.master.MasterConfig;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleVisitTokenManage extends AbstractDaemonService {
    private static final Logger logger = LoggerFactory.getLogger(SimpleVisitTokenManage.class);

    private final MasterConfig masterConfig;
    private final AtomicLong validVisitAuthorized = new AtomicLong(0);
    private final AtomicLong freshVisitAuthorized = new AtomicLong(0);

    public SimpleVisitTokenManage(final MasterConfig masterConfig) {
        super("[VisitToken Manager]", (masterConfig.getVisitTokenValidPeriodMs() * 4) / 5);
        this.masterConfig = masterConfig;
        freshVisitAuthorized.set(System.currentTimeMillis());
        validVisitAuthorized.set(freshVisitAuthorized.get());
        super.start();
    }

    public long getCurVisitToken() {
        return validVisitAuthorized.get();
    }

    public long getFreshVisitToken() {
        return freshVisitAuthorized.get();
    }


    @Override
    protected void loopProcess(long intervalMs) {
        while (!super.isStopped()) {
            try {
                Thread.sleep(intervalMs);
                validVisitAuthorized.set(freshVisitAuthorized.getAndSet(System.currentTimeMillis()));
            } catch (InterruptedException e) {
                logger.warn("[VisitToken Manager] Daemon generator thread has been interrupted");
                return;
            } catch (Throwable t) {
                logger.error("[VisitToken Manager] Daemon generator thread throw error ", t);
            }
        }
    }

    public void close(long waitTimeMs) {
        if (super.stop()) {
            return;
        }
        logger.info("[VisitToken Manager] VisitToken Manager service stopped!");
    }


}
