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

package com.tencent.tubemq.server.master.web.simplemvc.conf;

import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.master.web.simplemvc.Action;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


public class WebConfig {

    private final HashMap<String, Object> tools = new HashMap<String, Object>();
    private final HashMap<String, Action> actions = new HashMap<String, Action>();
    private final HashSet<String> types = new HashSet<String>();
    private String resourcePath;
    private String templatePath;
    private String actionPackage;
    private String velocityConfigFilePath;
    private String supportedTypes = ".htm,.html";
    private String defaultPage = "index.htm";
    private boolean springSupported = false;
    private List<String> beanFilePathList = new ArrayList<String>();
    private boolean standalone = false;

    public WebConfig() {
        parseTypes(this.supportedTypes);
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
        if (TStringUtils.isEmpty(this.templatePath)) {
            this.templatePath = ("/".equals(resourcePath) ? "" : resourcePath) + "/templates";
        }
    }

    public String getTemplatePath() {
        return templatePath;
    }

    public void setTemplatePath(String templatePath) {
        this.templatePath = templatePath;
    }

    public String getDefaultPage() {
        return defaultPage;
    }

    public void setDefaultPage(String defaultPage) {
        this.defaultPage = defaultPage;
    }

    public String getActionPackage() {
        return actionPackage;
    }

    public void setActionPackage(String actionPackage) {
        this.actionPackage = actionPackage;
    }

    public String getVelocityConfigFilePath() {
        return velocityConfigFilePath;
    }

    public void setVelocityConfigFilePath(String velocityConfigFilePath) {
        this.velocityConfigFilePath = velocityConfigFilePath;
    }

    public String getSupportedTypes() {
        return supportedTypes;
    }

    public void setSupportedTypes(String supportedTypes) {
        this.supportedTypes = supportedTypes;
        parseTypes(supportedTypes);
    }

    public HashMap<String, Object> getTools() {
        return tools;
    }

    public HashMap<String, Action> getActions() {
        return actions;
    }

    public void registerAction(Action action) {
        actions.put(action.getClass().getName(), action);
    }

    public void registerTool(String id, Object tool) {
        tools.put(id, tool);
    }

    public boolean containsType(String type) {
        return types.contains(type);
    }

    private void parseTypes(String typePattern) {
        String[] typeArr = typePattern.split(",");
        types.clear();
        for (String type : typeArr) {
            types.add(type);
        }
    }

    public List<String> getBeanFilePathList() {
        return beanFilePathList;
    }

    public void setBeanFilePathList(List<String> beanFilePathList) {
        this.beanFilePathList = beanFilePathList;
    }

    public boolean isSpringSupported() {
        return springSupported;
    }

    public void setSpringSupported(boolean springSupported) {
        this.springSupported = springSupported;
    }

    public boolean isStandalone() {
        return standalone;
    }

    public void setStandalone(boolean standalone) {
        this.standalone = standalone;
    }

    public void addBeanFilePath(String path) {
        this.beanFilePathList.add(path);
    }
}
