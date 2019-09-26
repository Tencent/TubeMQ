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

import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.master.web.simplemvc.conf.WebConfig;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class RequestContext extends MappedContext {

    private WebConfig config;

    private HttpServletRequest req;
    private HttpServletResponse resp;

    private String target;
    private String requestPath;
    private String requestType;
    private String redirectTarget;
    private String redirectLocation;

    public RequestContext(WebConfig config, HttpServletRequest req, HttpServletResponse resp) {
        this.config = config;
        this.req = req;
        this.resp = resp;
        this.requestPath =
                new StringBuilder(512).append(req.getServletPath())
                        .append(req.getPathInfo() != null ? req.getPathInfo() : "").toString();
        if ("/".equals(this.requestPath)) {
            this.requestPath = config.getDefaultPage();
        }
        if (this.requestPath.contains(".")) {
            this.requestType = this.requestPath.substring(this.requestPath.indexOf("."));
        }
        this.target = getRequestTarget();
    }

    private static String normalizePath(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1, path.length());
        }
        if (path.indexOf(".") > -1) {
            path = path.substring(0, path.indexOf("."));
        }
        return path;
    }

    public HttpServletRequest getReq() {
        return req;
    }

    public HttpServletResponse getResp() {
        return resp;
    }

    public WebConfig getConfig() {
        return config;
    }

    String getParameter(String name) {
        return req.getParameter(name);
    }

    Cookie[] getCookies() {
        return req.getCookies();
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getRedirectTarget() {
        return redirectTarget;
    }

    public void setRedirectTarget(String redirectTarget) {
        this.redirectTarget = redirectTarget;
    }

    public String getRedirectLocation() {
        return redirectLocation;
    }

    public void setRedirectLocation(String redirectLocation) {
        this.redirectLocation = redirectLocation;
    }

    boolean isRedirected() {
        return this.redirectTarget != null || this.redirectLocation != null;
    }

    public String requestType() {
        return this.requestType;
    }

    public String getRequestURI() {
        return req.getRequestURI();
    }

    private String getRequestTarget() {
        if (TStringUtils.isBlank(this.requestPath)) {
            return null;
        }
        String requestTarget = normalizePath(this.requestPath);
        int lastSlashIndex = requestTarget.lastIndexOf("/");
        if (lastSlashIndex >= 0) {
            requestTarget = new StringBuilder(512)
                    .append(requestTarget.substring(0, lastSlashIndex)).append("/")
                    .append(TStringUtils.toCamelCase(requestTarget.substring(lastSlashIndex + 1))).toString();
        } else {
            requestTarget = TStringUtils.toCamelCase(requestTarget);
        }

        return requestTarget;
    }
}
