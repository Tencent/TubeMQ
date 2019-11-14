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

package com.tencent.tubemq.corebase.policies;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flow control rule processing logic, including parsing the flow control json string,
 * obtaining the largest and smallest flow control values of each type to improve
 * the processing speed
 */
public class FlowCtrlRuleHandler {
    private final boolean isDefaultHandler;
    private final String flowCtrlName;
    private static final Logger logger =
            LoggerFactory.getLogger(FlowCtrlRuleHandler.class);
    private final TimeZone timeZone = TimeZone.getTimeZone("GMT+8:00");
    private final ReentrantLock writeLock = new ReentrantLock();
    // Flow control ID and string information obtained from the server
    private AtomicLong flowCtrlId =
            new AtomicLong(TBaseConstants.META_VALUE_UNDEFINED);
    private AtomicLong ssdTranslateId =
            new AtomicLong(TBaseConstants.META_VALUE_UNDEFINED);
    private AtomicInteger qryPriorityId =
            new AtomicInteger(TBaseConstants.META_VALUE_UNDEFINED);
    private String strFlowCtrlInfo;
    // The maximum interval of the flow control extracts the set of values,
    //improving the efficiency of the search return in the range
    private AtomicInteger minZeroCnt =
            new AtomicInteger(Integer.MAX_VALUE);
    private AtomicLong minSSDProcDlt =
            new AtomicLong(Long.MAX_VALUE);
    private AtomicInteger ssdLimitStartTime =
            new AtomicInteger(2500);
    private AtomicInteger ssdLimitEndTime =
            new AtomicInteger(TBaseConstants.META_VALUE_UNDEFINED);
    private AtomicLong minDataLimitDlt =
            new AtomicLong(Long.MAX_VALUE);
    private AtomicInteger dataLimitStartTime =
            new AtomicInteger(2500);
    private AtomicInteger dataLimitEndTime =
            new AtomicInteger(TBaseConstants.META_VALUE_UNDEFINED);
    private FlowCtrlItem filterCtrlItem =
            new FlowCtrlItem(3, TBaseConstants.META_VALUE_UNDEFINED,
                    TBaseConstants.META_VALUE_UNDEFINED, TBaseConstants.META_VALUE_UNDEFINED);
    private long lastUpdateTime =
            System.currentTimeMillis();
    // Decoded flow control rules
    private Map<Integer, List<FlowCtrlItem>> flowCtrlRuleSet =
            new ConcurrentHashMap<Integer, List<FlowCtrlItem>>();

    public FlowCtrlRuleHandler(boolean isDefault) {
        this.isDefaultHandler = isDefault;
        if (this.isDefaultHandler) {
            flowCtrlName = "Default_FlowCtrl";
        } else {
            flowCtrlName = "Group_FlowCtrl";
        }

    }

    /**
     * @param ssdTranslateId
     * @param qyrPriorityId
     * @param flowCtrlId
     * @param flowCtrlInfo
     * @throws Exception
     */
    public void updateDefFlowCtrlInfo(final long ssdTranslateId,
                                      final int qyrPriorityId,
                                      final long flowCtrlId,
                                      final String flowCtrlInfo) throws Exception {
        if (flowCtrlId == this.flowCtrlId.get()) {
            return;
        }
        Map<Integer, List<FlowCtrlItem>> flowCtrlItemsMap = null;
        if (TStringUtils.isNotBlank(flowCtrlInfo)) {
            flowCtrlItemsMap = parseFlowCtrlInfo(flowCtrlInfo);
        }
        writeLock.lock();
        try {
            this.flowCtrlId.set(flowCtrlId);
            this.strFlowCtrlInfo = flowCtrlInfo;
            logger.info(new StringBuilder(512)
                .append("[Flow Ctrl] Updated ").append(flowCtrlName)
                .append(" to flowId=").append(flowCtrlId)
                .append(",ssdTranslateId=").append(ssdTranslateId)
                .append(",qyrPriorityId=").append(qyrPriorityId).toString());
            this.ssdTranslateId.set(ssdTranslateId);
            this.qryPriorityId.set(qyrPriorityId);
            clearStatisData();
            if (flowCtrlItemsMap == null
                    || flowCtrlItemsMap.isEmpty()) {
                this.flowCtrlRuleSet.clear();
            } else {
                flowCtrlRuleSet = flowCtrlItemsMap;
                initialStatisData();
            }
            this.lastUpdateTime = System.currentTimeMillis();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @param lastDataDlt
     * @return FlowCtrlResult
     */
    public FlowCtrlResult getCurDataLimit(long lastDataDlt) {
        Calendar rightNow = Calendar.getInstance(timeZone);
        int hour = rightNow.get(Calendar.HOUR_OF_DAY);
        int minu = rightNow.get(Calendar.MINUTE);
        int curTime = hour * 100 + minu;
        if (lastDataDlt < this.minDataLimitDlt.get()
                || curTime < this.dataLimitStartTime.get()
                || curTime > this.dataLimitEndTime.get()) {
            return null;
        }
        List<FlowCtrlItem> flowCtrlItemList =
                flowCtrlRuleSet.get(0);
        if (flowCtrlItemList == null
                || flowCtrlItemList.isEmpty()) {
            return null;
        }
        for (FlowCtrlItem flowCtrlItem : flowCtrlItemList) {
            if (flowCtrlItem == null) {
                continue;
            }
            FlowCtrlResult flowCtrlResult =
                    flowCtrlItem.getDataLimit(lastDataDlt, hour, minu);
            if (flowCtrlResult != null) {
                return flowCtrlResult;
            }
        }
        return null;
    }


    public int getNormFreqInMs() {
        return this.filterCtrlItem.getFreqLtInMs();
    }

    public int getMinDataFreqInMs() {
        return this.filterCtrlItem.getZeroCnt();
    }

    public FlowCtrlItem getFilterCtrlItem() {
        return this.filterCtrlItem;
    }

    /**
     * initial statis data
     */
    private void initialStatisData() {
        initlDataLimitStatisInfo();
        initlFreqLimitStatisInfo();
        initlSSDProcLimitStatisInfo();
        initlLowFetchLimitStatisInfo();
    }

    /**
     * initial data limit statis info
     */
    private void initlDataLimitStatisInfo() {
        List<FlowCtrlItem> flowCtrlItemList = this.flowCtrlRuleSet.get(0);
        if (flowCtrlItemList != null
                && !flowCtrlItemList.isEmpty()) {
            for (FlowCtrlItem flowCtrlItem : flowCtrlItemList) {
                if (flowCtrlItem == null) {
                    continue;
                }
                if (flowCtrlItem.getType() != 0) {
                    continue;
                }
                if (flowCtrlItem.getDltInM() < this.minDataLimitDlt.get()) {
                    this.minDataLimitDlt.set(flowCtrlItem.getDltInM());
                }

                if (flowCtrlItem.getStartTime() < this.dataLimitStartTime.get()) {
                    this.dataLimitStartTime.set(flowCtrlItem.getStartTime());
                }
                if (flowCtrlItem.getEndTime() > this.dataLimitEndTime.get()) {
                    this.dataLimitEndTime.set(flowCtrlItem.getEndTime());
                }
            }
        }
    }

    private void initlFreqLimitStatisInfo() {
        List<FlowCtrlItem> flowCtrlItemList = flowCtrlRuleSet.get(1);
        if (flowCtrlItemList != null && !flowCtrlItemList.isEmpty()) {
            for (FlowCtrlItem flowCtrlItem : flowCtrlItemList) {
                if (flowCtrlItem == null) {
                    continue;
                }
                if (flowCtrlItem.getType() != 1) {
                    continue;
                }
                if (flowCtrlItem.getZeroCnt() < this.minZeroCnt.get()) {
                    this.minZeroCnt.set(flowCtrlItem.getZeroCnt());
                }
            }
        }
    }

    private void initlSSDProcLimitStatisInfo() {
        List<FlowCtrlItem> flowCtrlItemList = flowCtrlRuleSet.get(2);
        if (flowCtrlItemList != null && !flowCtrlItemList.isEmpty()) {
            for (FlowCtrlItem flowCtrlItem : flowCtrlItemList) {
                if (flowCtrlItem == null) {
                    continue;
                }
                if (flowCtrlItem.getType() != 2) {
                    continue;
                }
                if (flowCtrlItem.getDltInM() < this.minSSDProcDlt.get()) {
                    this.minSSDProcDlt.set(flowCtrlItem.getDltInM());
                }
                if (flowCtrlItem.getStartTime() < this.ssdLimitStartTime.get()) {
                    this.ssdLimitStartTime.set(flowCtrlItem.getStartTime());
                }
                if (flowCtrlItem.getEndTime() > this.ssdLimitEndTime.get()) {
                    this.ssdLimitEndTime.set(flowCtrlItem.getEndTime());
                }
            }
        }
    }

    private void initlLowFetchLimitStatisInfo() {
        List<FlowCtrlItem> flowCtrlItemList = flowCtrlRuleSet.get(3);
        if (flowCtrlItemList != null && !flowCtrlItemList.isEmpty()) {
            for (FlowCtrlItem flowCtrlItem : flowCtrlItemList) {
                if (flowCtrlItem == null) {
                    continue;
                }
                if (flowCtrlItem.getType() != 3) {
                    continue;
                }
                this.filterCtrlItem = new FlowCtrlItem(3,
                        (int) flowCtrlItem.getDataLtInSZ(),
                        flowCtrlItem.getFreqLtInMs(),
                        flowCtrlItem.getZeroCnt());
            }
        }
    }

    private void clearStatisData() {
        this.minZeroCnt.set(Integer.MAX_VALUE);
        this.minSSDProcDlt.set(Long.MAX_VALUE);
        this.minDataLimitDlt.set(Long.MAX_VALUE);
        this.ssdLimitStartTime.set(2500);
        this.ssdLimitEndTime.set(TBaseConstants.META_VALUE_UNDEFINED);
        this.dataLimitStartTime.set(2500);
        this.dataLimitEndTime.set(TBaseConstants.META_VALUE_UNDEFINED);
        this.filterCtrlItem = new FlowCtrlItem(3, TBaseConstants.META_VALUE_UNDEFINED,
                TBaseConstants.META_VALUE_UNDEFINED, TBaseConstants.META_VALUE_UNDEFINED);
    }


    public SSDCtrlResult getCurSSDStartDltInSZ() {
        Calendar rightNow = Calendar.getInstance(timeZone);
        int hour = rightNow.get(Calendar.HOUR_OF_DAY);
        int minu = rightNow.get(Calendar.MINUTE);
        int curTime = hour * 100 + minu;
        if (curTime < this.ssdLimitStartTime.get()
                || curTime > this.ssdLimitEndTime.get()) {
            return new SSDCtrlResult(Long.MAX_VALUE, 0);
        }
        List<FlowCtrlItem> flowCtrlItemList = flowCtrlRuleSet.get(2);
        if (flowCtrlItemList == null
                || flowCtrlItemList.isEmpty()) {
            return new SSDCtrlResult(Long.MAX_VALUE, 0);
        }
        for (FlowCtrlItem flowCtrlItem : flowCtrlItemList) {
            if (flowCtrlItem == null) {
                continue;
            }
            SSDCtrlResult ruleVal = flowCtrlItem.getSSDStartDltProc(hour, minu);
            if (ruleVal != null && ruleVal.dataStartDltInSize > 0) {
                return ruleVal;
            }
        }
        return new SSDCtrlResult(Long.MAX_VALUE, 0);
    }

    public int getMinZeroCnt() {
        return minZeroCnt.get();
    }

    /**
     * @param msgZeroCnt
     * @param rcmVal
     * @return
     */
    public int getCurFreqLimitTime(int msgZeroCnt, int rcmVal) {
        if (msgZeroCnt < this.minZeroCnt.get()) {
            return rcmVal;
        }
        List<FlowCtrlItem> flowCtrlItemList =
                flowCtrlRuleSet.get(1);
        if (flowCtrlItemList == null
                || flowCtrlItemList.isEmpty()) {
            return rcmVal;
        }
        for (FlowCtrlItem flowCtrlItem : flowCtrlItemList) {
            if (flowCtrlItem == null) {
                continue;
            }
            int ruleVal = flowCtrlItem.getFreLimit(msgZeroCnt);
            if (ruleVal >= 0) {
                return ruleVal;
            }
        }
        return rcmVal;
    }

    public long getSsdTranslateId() {
        return ssdTranslateId.get();
    }


    /**
     * @param ssdTranslateId
     */
    public void setSsdTranslateId(long ssdTranslateId) {
        this.ssdTranslateId.set(ssdTranslateId);
    }

    public int getQryPriorityId() {
        return qryPriorityId.get();
    }


    /**
     * @param qryPriorityId
     */
    public void setQryPriorityId(int qryPriorityId) {
        this.qryPriorityId.set(qryPriorityId);
    }

    public long getFlowCtrlId() {
        return flowCtrlId.get();
    }

    public void clear() {
        writeLock.lock();
        try {
            this.strFlowCtrlInfo = "";
            this.flowCtrlRuleSet.clear();
            this.flowCtrlId.set(TBaseConstants.META_VALUE_UNDEFINED);
            this.ssdTranslateId.set(TBaseConstants.META_VALUE_UNDEFINED);
            this.qryPriorityId.set(TBaseConstants.META_VALUE_UNDEFINED);
        } finally {
            writeLock.unlock();
        }
    }


    /**
     * @param flowCtrlInfo
     * @return
     * @throws Exception
     */
    public Map<Integer, List<FlowCtrlItem>> parseFlowCtrlInfo(final String flowCtrlInfo)
            throws Exception {
        Map<Integer, List<FlowCtrlItem>> flowCtrlMap = new ConcurrentHashMap<Integer, List<FlowCtrlItem>>();
        if (TStringUtils.isBlank(flowCtrlInfo)) {
            throw new Exception("Parsing error, flowCtrlInfo value is blank!");
        }
        JSONArray objArray = null;
        try {
            objArray = JSON.parseArray(flowCtrlInfo);
        } catch (Throwable e1) {
            throw new Exception("Parse flowCtrlInfo value failure", e1);
        }
        if (objArray == null) {
            throw new Exception("Parsing error, flowCtrlInfo value must be valid json format!");
        }
        if (objArray.size() == 0) {
            return flowCtrlMap;
        }
        try {
            List<FlowCtrlItem> flowCtrlItemList;
            for (int i = 0; i < objArray.size(); i++) {
                JSONObject jsonObject = objArray.getJSONObject(i);
                int typeVal = jsonObject.getIntValue("type");
                if (typeVal < 0 || typeVal > 3) {
                    throw new Exception(new StringBuilder(512)
                            .append("type value must in [0,1,2,3] in index(")
                            .append(i).append(") of flowCtrlInfo value!").toString());
                }
                switch (typeVal) {
                    case 1:
                        flowCtrlItemList = parseFreqLimit(typeVal, jsonObject);
                        break;

                    case 2:
                        flowCtrlItemList = parseSSDProcLimit(typeVal, jsonObject);
                        break;

                    case 3:
                        flowCtrlItemList = parseLowFetchLimit(typeVal, jsonObject);
                        break;

                    case 0:
                    default:
                        typeVal = 0;
                        flowCtrlItemList = parseDataLimit(typeVal, jsonObject);
                        break;
                }
                if (flowCtrlItemList != null && !flowCtrlItemList.isEmpty()) {
                    flowCtrlMap.put(typeVal, flowCtrlItemList);
                }
            }
        } catch (Throwable e2) {
            throw new Exception(new StringBuilder(512).append("Parse flow-ctrl rule failure, ")
                    .append(e2.getMessage()).toString());
        }
        return flowCtrlMap;
    }


    /**
     * lizard forgives
     *
     * @param typeVal
     * @param jsonObject
     * @return
     * @throws Exception
     */
    private List<FlowCtrlItem> parseDataLimit(int typeVal, JSONObject jsonObject) throws Exception {
        if (jsonObject == null || jsonObject.getIntValue("type") != 0) {
            throw new Exception("parse data limit rule failure!");
        }
        JSONArray ruleArray = jsonObject.getJSONArray("rule");
        if (ruleArray == null) {
            throw new Exception("not found rule list in data limit!");
        }
        ArrayList<FlowCtrlItem> flowCtrlItems = new ArrayList<FlowCtrlItem>();
        for (int index = 0; index < ruleArray.size(); index++) {
            JSONObject ruleObject = ruleArray.getJSONObject(index);
            int startTime = validAndGetTimeValue("start",
                    ruleObject.getString("start"), index, "data");
            int endTime = validAndGetTimeValue("end",
                    ruleObject.getString("end"), index, "data");
            if (startTime >= endTime) {
                throw new Exception(new StringBuilder(512)
                        .append("start value must lower than the End value in index(")
                        .append(index).append(") of data limit rule!").toString());
            }
            if (!ruleObject.containsKey("dltInM")) {
                throw new Exception(new StringBuilder(512)
                        .append("dltInM key is required in index(")
                        .append(index).append(") of data limit rule!").toString());
            }
            long dltVal = ruleObject.getLong("dltInM");
            if (dltVal <= 20) {
                throw new Exception(new StringBuilder(512)
                        .append("dltInM value must be greater than 20 in index(")
                        .append(index).append(") of data limit rule!").toString());
            }
            if (!ruleObject.containsKey("limitInM")) {
                throw new Exception(new StringBuilder(512)
                        .append("limitInM key is required in index(")
                        .append(index).append(") of data limit rule!").toString());
            }
            long dataLimitInM = ruleObject.getLong("limitInM");
            if (dataLimitInM < 0) {
                throw new Exception(new StringBuilder(512)
                        .append("limitInM value must be greater than or equal to zero in index(")
                        .append(index).append(") of data limit rule!").toString());
            }
            dataLimitInM = dataLimitInM * 1024 * 1024;
            if (!ruleObject.containsKey("freqInMs")) {
                throw new Exception(new StringBuilder(512)
                        .append("freqInMs key is required in index(")
                        .append(index).append(") of data limit rule!").toString());
            }
            int freqInMs = ruleObject.getIntValue("freqInMs");
            if (freqInMs < 200) {
                throw new Exception(new StringBuilder(512)
                        .append("freqInMs value must be greater than or equal to 200 in index(")
                        .append(index).append(") of data limit rule!").toString());
            }
            flowCtrlItems.add(new FlowCtrlItem(typeVal,
                    startTime, endTime, dltVal, dataLimitInM, freqInMs));
        }

        Collections.sort(flowCtrlItems, new Comparator<FlowCtrlItem>() {
            @Override
            public int compare(final FlowCtrlItem o1, final FlowCtrlItem o2) {
                if (o1.getStartTime() > o2.getStartTime()) {
                    return 1;
                } else if (o1.getStartTime() < o2.getStartTime()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
        return flowCtrlItems;
    }

    /**
     * @param typeVal
     * @param jsonObject
     * @return
     * @throws Exception
     */
    private List<FlowCtrlItem> parseFreqLimit(int typeVal,
                                              JSONObject jsonObject) throws Exception {
        if (jsonObject == null || jsonObject.getIntValue("type") != 1) {
            throw new Exception("parse freq limit rule failure!");
        }
        JSONArray ruleArray = jsonObject.getJSONArray("rule");
        if (ruleArray == null) {
            throw new Exception("not found rule list in freq limit!");
        }
        ArrayList<FlowCtrlItem> flowCtrlItems = new ArrayList<FlowCtrlItem>();
        for (int index = 0; index < ruleArray.size(); index++) {
            JSONObject ruleObject = ruleArray.getJSONObject(index);
            if (!ruleObject.containsKey("zeroCnt")) {
                throw new Exception(new StringBuilder(512)
                        .append("zeroCnt key is required in index(")
                        .append(index).append(") of freq limit rule!").toString());
            }
            int zeroCnt = ruleObject.getIntValue("zeroCnt");
            if (zeroCnt < 1) {
                throw new Exception(new StringBuilder(512)
                        .append("zeroCnt value must be greater than or equal to 1 in index(")
                        .append(index).append(") of freq limit rule!").toString());
            }
            if (!ruleObject.containsKey("freqInMs")) {
                throw new Exception(new StringBuilder(512)
                        .append("freqInMs key is required in index(")
                        .append(index).append(") of freq limit rule!").toString());
            }
            int freqInMs = ruleObject.getIntValue("freqInMs");
            if (freqInMs < 0) {
                throw new Exception(new StringBuilder(512)
                        .append("freqInMs value must be greater than or equal to zero in index(")
                        .append(index).append(") of freq limit rule!").toString());
            }
            flowCtrlItems.add(new FlowCtrlItem(typeVal, zeroCnt, freqInMs));
        }

        Collections.sort(flowCtrlItems, new Comparator<FlowCtrlItem>() {
            @Override
            public int compare(final FlowCtrlItem o1, final FlowCtrlItem o2) {
                if (o1.getZeroCnt() > o2.getZeroCnt()) {
                    return -1;
                } else if (o1.getZeroCnt() < o2.getZeroCnt()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
        return flowCtrlItems;
    }

    /**
     * @param typeVal
     * @param jsonObject
     * @return
     * @throws Exception
     */
    private List<FlowCtrlItem> parseLowFetchLimit(int typeVal,
                                                  JSONObject jsonObject) throws Exception {
        if (jsonObject == null || jsonObject.getIntValue("type") != 3) {
            throw new Exception("parse low fetch limit rule failure!");
        }
        JSONArray ruleArray = jsonObject.getJSONArray("rule");
        if (ruleArray == null) {
            throw new Exception("not found rule list in low fetch limit!");
        }
        if (ruleArray.size() > 1) {
            throw new Exception("only allow set one rule in low fetch limit!");
        }
        ArrayList<FlowCtrlItem> flowCtrlItems = new ArrayList<FlowCtrlItem>();
        for (int index = 0; index < ruleArray.size(); index++) {
            JSONObject ruleObject = ruleArray.getJSONObject(index);
            int normfreqInMs = 0;
            int filterFreqInMs = 0;
            int minDataFilterFreqInMs = 0;
            if (ruleObject.containsKey("filterFreqInMs")
                    || ruleObject.containsKey("minDataFilterFreqInMs")) {
                filterFreqInMs = ruleObject.getIntValue("filterFreqInMs");
                if (filterFreqInMs < 0 || filterFreqInMs > 300000) {
                    throw new Exception(new StringBuilder(512)
                            .append("filterFreqInMs value must in [0, 300000] in index(")
                            .append(index).append(") of low fetch limit rule!").toString());
                }
                if (!ruleObject.containsKey("minDataFilterFreqInMs")) {
                    throw new Exception(new StringBuilder(512)
                            .append("minDataFilterFreqInMs key is required in index(")
                            .append(index).append(") of low fetch limit rule!").toString());
                }
                minDataFilterFreqInMs = ruleObject.getIntValue("minDataFilterFreqInMs");
                if (minDataFilterFreqInMs < 0 || minDataFilterFreqInMs > 300000) {
                    throw new Exception(new StringBuilder(512)
                            .append("minDataFilterFreqInMs value must in [0, 300000] in index(")
                            .append(index).append(") of low fetch limit rule!").toString());
                }
                if (minDataFilterFreqInMs < filterFreqInMs) {
                    throw new Exception(new StringBuilder(512)
                            .append("minDataFilterFreqInMs value must be greater than ")
                            .append("or equal to filterFreqInMs value in index(")
                            .append(index).append(") of low fetch limit rule!").toString());
                }
            }
            if (ruleObject.containsKey("normFreqInMs")) {
                normfreqInMs = ruleObject.getIntValue("normFreqInMs");
                if (normfreqInMs < 0 || normfreqInMs > 300000) {
                    throw new Exception(new StringBuilder(512)
                            .append("normFreqInMs value must in [0, 300000] in index(")
                            .append(index).append(") of low fetch limit rule!").toString());
                }
            }
            flowCtrlItems.add(new FlowCtrlItem(typeVal,
                    normfreqInMs, filterFreqInMs, minDataFilterFreqInMs));
        }

        Collections.sort(flowCtrlItems, new Comparator<FlowCtrlItem>() {
            @Override
            public int compare(final FlowCtrlItem o1, final FlowCtrlItem o2) {
                if (o1.getFreqLtInMs() > o2.getFreqLtInMs()) {
                    return -1;
                } else if (o1.getFreqLtInMs() < o2.getFreqLtInMs()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
        return flowCtrlItems;
    }

    /**
     * @param typeVal
     * @param jsonObject
     * @return
     * @throws Exception
     */
    private List<FlowCtrlItem> parseSSDProcLimit(int typeVal,
                                                 JSONObject jsonObject) throws Exception {
        if (jsonObject == null || jsonObject.getIntValue("type") != 2) {
            throw new Exception("parse SSD limit rule failure!");
        }
        JSONArray ruleArray = jsonObject.getJSONArray("rule");
        if (ruleArray == null) {
            throw new Exception("not found rule list in SSD limit!");
        }
        ArrayList<FlowCtrlItem> flowCtrlItems = new ArrayList<FlowCtrlItem>();
        for (int index = 0; index < ruleArray.size(); index++) {
            JSONObject ruleObject = ruleArray.getJSONObject(index);
            int startTime = validAndGetTimeValue("start",
                    ruleObject.getString("start"), index, "SSD");
            int endTime = validAndGetTimeValue("end",
                    ruleObject.getString("end"), index, "SSD");
            if (startTime >= endTime) {
                throw new Exception(new StringBuilder(512)
                        .append("start value must be less than End value in index(")
                        .append(index).append(") of SSD limit rule!").toString());
            }
            if (!ruleObject.containsKey("dltStInM")) {
                throw new Exception(new StringBuilder(512)
                        .append("dltStInM key is required in index(")
                        .append(index).append(") of SSD limit rule!").toString());
            }
            long dltStInM = ruleObject.getLong("dltStInM");
            if (dltStInM < 512) {
                throw new Exception(new StringBuilder(512)
                        .append("dltStInM value must be greater than or equal to 512 in index(")
                        .append(index).append(") of SSD limit rule!").toString());
            }
            if (!ruleObject.containsKey("dltEdInM")) {
                throw new Exception(new StringBuilder(512)
                        .append("dltEdInM key is required in index(")
                        .append(index).append(") of SSD limit rule!").toString());
            }
            long dataEndInM = ruleObject.getLong("dltEdInM");
            if (dataEndInM < 0) {
                throw new Exception(new StringBuilder(512)
                        .append("dltEdInM value must be greater than or equal to zero in index(")
                        .append(index).append(") of SSD limit rule!").toString());
            }
            if (dataEndInM < 512) {
                throw new Exception(new StringBuilder(512)
                        .append("dltStInM value must be greater than or equal to 512 in index(")
                        .append(index).append(") of SSD limit rule!").toString());
            }
            if (dltStInM < dataEndInM) {
                throw new Exception(new StringBuilder(512)
                        .append("dltStInM value must be greater than ")
                        .append("or equal to dltEdInM value in index(")
                        .append(index).append(") of SSD limit rule!").toString());
            }
            dltStInM = (long) (dltStInM * 1024 * 1024);
            dataEndInM = (long) (dataEndInM * 1024 * 1024);
            flowCtrlItems.add(new FlowCtrlItem(typeVal, startTime, endTime, dltStInM, dataEndInM));
        }

        Collections.sort(flowCtrlItems, new Comparator<FlowCtrlItem>() {
            @Override
            public int compare(final FlowCtrlItem o1, final FlowCtrlItem o2) {
                if (o1.getStartTime() > o2.getStartTime()) {
                    return 1;
                } else if (o1.getStartTime() < o2.getStartTime()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
        return flowCtrlItems;
    }

    public String toString() {
        return this.strFlowCtrlInfo;
    }


    /**
     * @param strValName
     * @param strTimeVal
     * @param index
     * @param ruleType
     * @return
     * @throws Exception
     */
    private int validAndGetTimeValue(final String strValName,
                                     final String strTimeVal,
                                     int index, final String ruleType) throws Exception {
        if (TStringUtils.isBlank(strTimeVal)) {
            throw new Exception(strValName + " value is null or blank of "
                    + ruleType + " limit rule!");
        }
        int timeHour = 0;
        int timeMin = 0;
        String[] startItems = strTimeVal.split(TokenConstants.ATTR_SEP);
        if ((startItems.length != 2)
                || TStringUtils.isBlank(startItems[0])
                || TStringUtils.isBlank(startItems[1])) {
            throw new Exception("illegal format, " + strValName
                    + " value must be 'aa:bb' and 'aa','bb' must be int value format in "
                    + ruleType + " limit rule!");
        }
        try {
            timeHour = Integer.valueOf(startItems[0]);
        } catch (Throwable e2) {
            throw new Exception("illegal format, " + strValName
                    + " value must be 'aa:bb' and 'aa' must be int value in "
                    + ruleType + " limit rule!");
        }
        try {
            timeMin = Integer.valueOf(startItems[1]);
        } catch (Throwable e2) {
            throw new Exception("illegal format, " + strValName
                    + " value must be 'aa:bb' and 'bb' must be int value in "
                    + ruleType + " limit rule!");
        }
        if (timeHour < 0 || timeHour > 24) {
            throw new Exception(new StringBuilder(512)
                    .append(strValName).append("-hour value must in [0,23] in index(")
                    .append(index).append(") of ").append(ruleType).append(" limit rule!").toString());
        }
        if (timeMin < 0 || timeMin > 59) {
            throw new Exception(new StringBuilder(512)
                    .append(strValName).append("-minute value must in [0,59] in index(")
                    .append(index).append(") of ").append(ruleType).append(" limit rule!").toString());
        }
        return timeHour * 100 + timeMin;
    }
}
