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
import com.tencent.tubemq.server.master.web.simplemvc.conf.ConfigFileParser;
import com.tencent.tubemq.server.master.web.simplemvc.conf.WebConfig;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(WebFilter.class);

    private static final String DEFAULT_CONFIG_PATH = "/WEB-INF/simple-mvc.xml";
    private static final String DEFAULT_LOG_CONFIG_PATH = "/WEB-INF/log4j.xml";
    private String configFilePath;
    private File configFile;
    private WebConfig config;

    private RequestDispatcher dispatcher;

    public WebFilter() {
    }

    public WebFilter(String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public WebFilter(WebConfig config) {
        this.config = config;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        try {
            initLogSystem(filterConfig);
            if (this.config == null) {
                if (this.configFilePath == null) {
                    String filePath = filterConfig.getInitParameter("configFile");
                    if (TStringUtils.isEmpty(filePath)) {
                        filePath = DEFAULT_CONFIG_PATH;
                    }
                    this.configFilePath = filePath;
                }
                URL configFileURL =
                        filterConfig.getServletContext().getResource(this.configFilePath);
                if (configFileURL == null) {
                    throw new ServletException(new StringBuilder(256)
                            .append("can not found config file:")
                            .append(this.configFilePath).toString());
                }
                this.configFile = new File(configFileURL.toURI());
                ConfigFileParser configParser = new ConfigFileParser(this.configFile);
                this.config = configParser.parse();
            }
            checkConfig(this.config, filterConfig.getServletContext());
            this.dispatcher = new RequestDispatcher(config);
            this.dispatcher.init();
        } catch (ServletException se) {
            throw se;
        } catch (Throwable t) {
            logger.error("Dispatcher start failed!", t);
            throw new ServletException(t);
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;

        String charset = (req.getCharacterEncoding() == null)
                ? TBaseConstants.META_DEFAULT_CHARSET_NAME : req.getCharacterEncoding();
        resp.setCharacterEncoding(charset);
        RequestContext context = new RequestContext(this.config, req, resp);
        if (this.config.containsType(context.requestType())) {
            if (dispatcher == null) {
                resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            } else {
                try {
                    dispatcher.processRequest(context);
                } catch (Throwable t) {
                    logger.error("", t);
                    resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                }
            }
        } else {
            chain.doFilter(request, response);
        }
        resp.flushBuffer();
    }

    private void checkConfig(WebConfig config, ServletContext servletContext) throws Exception {
        URL resourcesURL =
                servletContext.getResource(config.getResourcePath());
        if (resourcesURL == null) {
            throw new ServletException(new StringBuilder(256)
                    .append("Invalid resources path:")
                    .append(config.getResourcePath()).toString());
        }
        config.setResourcePath(resourcesURL.getPath());
        URL templatesURL = servletContext.getResource(config.getTemplatePath());
        if (templatesURL == null) {
            throw new ServletException(new StringBuilder(256)
                    .append("Invalid templates path:")
                    .append(config.getTemplatePath()).toString());
        }
        config.setTemplatePath(templatesURL.getPath());

        if (TStringUtils.isNotEmpty(config.getVelocityConfigFilePath())) {
            URL velocityConfigFilePath =
                    servletContext.getResource(config.getVelocityConfigFilePath());
            if (velocityConfigFilePath != null) {
                config.setVelocityConfigFilePath(velocityConfigFilePath.getPath());
            } else {
                logger.warn(new StringBuilder(256)
                        .append("Invalid velocity config file path:")
                        .append(config.getVelocityConfigFilePath()).toString());
                config.setVelocityConfigFilePath(null);
            }
        }
    }

    private void initLogSystem(FilterConfig filterConfig) throws Exception {
        String filePath = filterConfig.getInitParameter("logConfigFile");
        if (TStringUtils.isEmpty(filePath) && !this.config.isStandalone()) {
            filePath = DEFAULT_LOG_CONFIG_PATH;
        }
        if (TStringUtils.isNotEmpty(filePath)) {
            ServletContext servletContext = filterConfig.getServletContext();
            if (servletContext != null) {
                URL logConfigFileURL = servletContext.getResource(filePath);
                if (logConfigFileURL != null) {
                    PropertyConfigurator.configure(logConfigFileURL);
                }
            }
        }
    }
}
