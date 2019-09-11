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

package com.tencent.tubemq.server.master.web;

import com.tencent.tubemq.server.Server;
import com.tencent.tubemq.server.master.MasterConfig;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.web.action.layout.Default;
import com.tencent.tubemq.server.master.web.action.screen.Master;
import com.tencent.tubemq.server.master.web.action.screen.Tubeweb;
import com.tencent.tubemq.server.master.web.action.screen.Webapi;
import com.tencent.tubemq.server.master.web.action.screen.cluster.ClusterManage;
import com.tencent.tubemq.server.master.web.action.screen.config.BrokerDetail;
import com.tencent.tubemq.server.master.web.action.screen.config.BrokerList;
import com.tencent.tubemq.server.master.web.action.screen.config.TopicDetail;
import com.tencent.tubemq.server.master.web.action.screen.config.TopicList;
import com.tencent.tubemq.server.master.web.action.screen.consume.Detail;
import com.tencent.tubemq.server.master.web.simplemvc.WebFilter;
import com.tencent.tubemq.server.master.web.simplemvc.conf.WebConfig;
import org.apache.velocity.tools.generic.DateTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;


public class WebServer implements Server {

    private final MasterConfig masterConfig;
    private org.mortbay.jetty.Server srv;
    private TMaster master;

    public WebServer(final MasterConfig masterConfig, TMaster master) {
        this.masterConfig = masterConfig;
        this.master = master;
    }

    @Override
    public void start() throws Exception {
        WebConfig webConfig = new WebConfig();
        webConfig.setActionPackage("com.tencent.tubemq.server.master.web.action");
        webConfig.setResourcePath("/");
        webConfig.setVelocityConfigFilePath("/velocity.properties");
        webConfig.setStandalone(true);
        registerActions(webConfig);
        registerTools(webConfig);
        srv = new org.mortbay.jetty.Server(masterConfig.getWebPort());
        org.mortbay.jetty.servlet.Context servletContext =
                new org.mortbay.jetty.servlet.Context(srv,
                        "/", org.mortbay.jetty.servlet.Context.SESSIONS);
        servletContext.addFilter(new FilterHolder(
                new MasterStatusCheckFilter(master)), "/*", Handler.REQUEST);
        servletContext.addFilter(new FilterHolder(
                new UserAuthFilter()), "/*", Handler.REQUEST);
        FilterHolder filterHolder =
                new FilterHolder(new WebFilter(webConfig));
        servletContext.addFilter(filterHolder, "/*", Handler.REQUEST);
        DefaultServlet defaultServlet = new DefaultServlet();
        ServletHolder servletHolder = new ServletHolder(defaultServlet);
        servletHolder.setInitParameter("dirAllowed", "false");
        servletContext.addServlet(servletHolder, "/*");
        servletContext.setResourceBase(masterConfig.getWebResourcePath());
        srv.start();
        if (!srv.getHandler().equals(servletContext)) {
            throw new Exception("servletContext is not a handler!");
        }
    }

    @Override
    public void stop() throws Exception {
        srv.stop();
    }

    private void registerActions(WebConfig config) {
        config.registerAction(new Detail(this.master));
        config.registerAction(new BrokerDetail(this.master));
        config.registerAction(new TopicDetail(this.master));
        config.registerAction(new TopicList(this.master));
        config.registerAction(new ClusterManage(this.master));
        config.registerAction(new BrokerList(this.master));
        config.registerAction(new Master(this.master));
        config.registerAction(new Webapi(this.master));
        config.registerAction(new Tubeweb(this.master));
        config.registerAction(new Default(this.master));
    }

    private void registerTools(WebConfig config) {
        config.registerTool("dateTool", new DateTool());
        config.registerTool("numericTool", new NumberTool());
    }
}

