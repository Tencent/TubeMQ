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

package com.tencent.tubemq.server.broker.msgstore.ssd;

/***
 * SSD visit record.
 */
public class SSDVisitInfo {
    public final String partStr;
    public long lastOffset;
    public long lastTime;

    public SSDVisitInfo(final String partStr, final long lastOffset) {
        this.partStr = partStr;
        this.lastOffset = lastOffset;
        this.lastTime = System.currentTimeMillis();
    }

    public void requestVisit(final long lastOffset) {
        this.lastOffset = lastOffset;
        this.lastTime = System.currentTimeMillis();
    }

    public void responseVisit(final long lastOffset) {
        this.lastOffset = lastOffset;
        this.lastTime = System.currentTimeMillis();
    }

}
