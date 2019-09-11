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

package com.tencent.tubemq.server.master.web.simplemvc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControlTool {

    private static final Logger logger =
            LoggerFactory.getLogger(ControlTool.class);

    private RequestDispatcher dispatcher;

    private RequestContext requestContext;

    public ControlTool(RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void setTemplate(String templatePath) {
        try {
            String target = templatePath.substring(0, templatePath.indexOf(".vm"));
            dispatcher.executeTarget(requestContext, target, "control");
        } catch (Exception e) {
            logger.error("Render control template error!", e);
        }
    }

    public void setRequestContext(RequestContext requestContext) {
        this.requestContext = requestContext;
    }
}
