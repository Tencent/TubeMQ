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

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.master.web.simplemvc.conf.WebConfig;
import java.io.StringWriter;
import java.io.Writer;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;


public class VelocityTemplateEngine implements TemplateEngine {

    private WebConfig config;
    private VelocityEngine engine;

    public VelocityTemplateEngine(WebConfig config) {
        this.config = config;
        this.engine = new VelocityEngine();
    }

    @Override
    public void init() throws Exception {
        if (TStringUtils.isEmpty(config.getVelocityConfigFilePath())) {
            engine.setProperty(VelocityEngine.FILE_RESOURCE_LOADER_PATH, config.getTemplatePath());
            engine.setProperty(VelocityEngine.FILE_RESOURCE_LOADER_CACHE, false);
            engine.setProperty(VelocityEngine.INPUT_ENCODING, TBaseConstants.META_DEFAULT_CHARSET_NAME);
            engine.setProperty(VelocityEngine.OUTPUT_ENCODING, TBaseConstants.META_DEFAULT_CHARSET_NAME);
            engine.init();
        } else {
            engine.init(config.getVelocityConfigFilePath());
        }
    }

    @Override
    public String renderTemplate(String templateName,
                                 RequestContext context) throws Exception {
        StringWriter writer = new StringWriter();
        renderTemplate(templateName, context, writer);
        return writer.toString();
    }

    @Override
    public void renderTemplate(String templateName,
                               RequestContext context,
                               Writer writer) throws Exception {
        Template t = engine.getTemplate(templateName);
        if (t != null) {
            t.merge(new VelocityContext(context.getMap()), writer);
        }
    }
}
