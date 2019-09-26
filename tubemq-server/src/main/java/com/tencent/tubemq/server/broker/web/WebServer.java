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

package com.tencent.tubemq.server.broker.web;

import static com.google.common.base.Preconditions.checkArgument;
import com.tencent.tubemq.server.Server;
import com.tencent.tubemq.server.broker.TubeBroker;
import org.mortbay.jetty.servlet.ServletHolder;

/***
 * Broker's http server.
 */
public class WebServer implements Server {

    private String hostname = "0.0.0.0";
    private int port = 8080;
    private org.mortbay.jetty.Server srv;
    private TubeBroker broker;

    public WebServer(String hostname, int port, TubeBroker broker) {
        this.hostname = hostname;
        this.port = port;
        this.broker = broker;
    }

    @Override
    public void start() throws Exception {
        srv = new org.mortbay.jetty.Server(this.port);
        org.mortbay.jetty.servlet.Context servletContext =
                new org.mortbay.jetty.servlet.Context(srv, "/", org.mortbay.jetty.servlet.Context.SESSIONS);

        servletContext.addServlet(new ServletHolder(new BrokerAdminServlet(broker)), "/*");
        srv.start();
        checkArgument(srv.getHandler().equals(servletContext));
    }

    @Override
    public void stop() throws Exception {
        srv.stop();
    }
}
