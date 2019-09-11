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

package com.tencent.tubemq.server.master.bdbstore;


public class MasterGroupStatus {
    private boolean isMaster = false;
    private boolean isWritable = false;
    private boolean isReadable = false;

    public MasterGroupStatus() {

    }

    public MasterGroupStatus(final boolean isMaster) {
        this.isMaster = isMaster;
    }

    public MasterGroupStatus(final boolean isMaster, final boolean isWritable,
                             final boolean isReadable) {
        this.isMaster = isMaster;
        this.isWritable = isWritable;
        this.isReadable = isReadable;
    }

    public void setMasterGroupStatus(final boolean isMaster, final boolean isWritable,
                                     final boolean isReadable) {
        this.isMaster = isMaster;
        this.isWritable = isWritable;
        this.isReadable = isReadable;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean isMaster) {
        this.isMaster = isMaster;
    }

    public boolean isWritable() {
        return isWritable;
    }

    public void setWritable(boolean isWritable) {
        this.isWritable = isWritable;
    }

    public boolean isReadable() {
        return isReadable;
    }


    public void setReadable(boolean isReadable) {
        this.isReadable = isReadable;
    }
}
