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

import java.io.File;

/***
 * SSD segment found result.
 */
public class SSDSegFound {
    public final File sourceFile;
    public final long startOffset;
    public final long endOffset;
    public boolean isSuccess;
    public int reason;

    public SSDSegFound(boolean isSuccess, int reason, final File sourceFile) {
        this.isSuccess = isSuccess;
        this.reason = reason;
        this.sourceFile = sourceFile;
        this.startOffset = -2;
        this.endOffset = -2;
    }

    public SSDSegFound(boolean isSuccess, int reason, final File sourceFile,
                       final long startOffset, final long endOffset) {
        this.isSuccess = isSuccess;
        this.reason = reason;
        this.sourceFile = sourceFile;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

}
