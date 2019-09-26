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

import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractSamplePrint {
    private static final Logger logger = LoggerFactory.getLogger(AbstractSamplePrint.class);
    protected final StringBuilder sBuilder = new StringBuilder(512);
    protected AtomicLong lastLogTime = new AtomicLong(0);
    protected AtomicLong totalPrintCount = new AtomicLong(0);
    protected long sampleDetailDur = 30000;
    protected long maxDetailCount = 10;
    protected long sampleResetDur = 1200000;
    protected long maxTotalCount = 15;
    protected long maxUncheckDetailCount = 15;
    protected AtomicLong totalUncheckCount = new AtomicLong(10);


    public AbstractSamplePrint() {

    }

    public AbstractSamplePrint(long sampleDetailDur,
                               long sampleResetDur,
                               long maxDetailCount,
                               long maxTotalCount) {
        this.sampleDetailDur = sampleDetailDur;
        this.sampleResetDur = sampleResetDur;
        this.maxDetailCount = maxDetailCount;
        this.maxTotalCount = maxTotalCount;
        this.maxUncheckDetailCount = maxDetailCount;
    }

    public abstract void printExceptionCaught(Throwable e);

    public abstract void printExceptionCaught(Throwable e, String hostName, String nodeName);
}
