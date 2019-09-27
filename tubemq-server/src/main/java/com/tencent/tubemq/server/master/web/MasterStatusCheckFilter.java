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

import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import java.io.IOException;
import java.net.InetSocketAddress;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class MasterStatusCheckFilter implements Filter {

    private TMaster master;
    private BrokerConfManage brokerConfManage;

    public MasterStatusCheckFilter(TMaster master) {
        this.master = master;
        this.brokerConfManage =
                this.master.getMasterTopicManage();
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;
        if (!brokerConfManage.isSelfMaster()) {
            InetSocketAddress masterAddr =
                    brokerConfManage.getMasterAddress();
            if (masterAddr == null) {
                throw new IOException("Not found the master node address!");
            }
            StringBuilder sBuilder = new StringBuilder(512).append("http://")
                    .append(masterAddr.getAddress().getHostAddress())
                    .append(":").append(master.getMasterConfig().getWebPort())
                    .append(req.getRequestURI());
            if (TStringUtils.isNotBlank(req.getQueryString())) {
                sBuilder.append("?").append(req.getQueryString());
            }
            resp.sendRedirect(sBuilder.toString());
            return;
        }
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
