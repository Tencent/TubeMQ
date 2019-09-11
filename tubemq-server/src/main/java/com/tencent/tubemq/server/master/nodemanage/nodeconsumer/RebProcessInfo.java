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

package com.tencent.tubemq.server.master.nodemanage.nodeconsumer;

import java.util.ArrayList;
import java.util.List;


public class RebProcessInfo {

    public List<String> needProcessList =
            new ArrayList<String>();
    public List<String> needEscapeList =
            new ArrayList<String>();

    public RebProcessInfo() {

    }

    public RebProcessInfo(List<String> needProcessList,
                          List<String> needEscapeList) {
        this.needProcessList = needProcessList;
        this.needEscapeList = needEscapeList;
    }

    public boolean isProcessInfoEmpty() {
        return (this.needProcessList.isEmpty()
                && this.needEscapeList.isEmpty());
    }
}
