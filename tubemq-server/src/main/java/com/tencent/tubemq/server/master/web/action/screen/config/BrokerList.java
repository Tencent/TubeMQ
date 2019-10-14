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

package com.tencent.tubemq.server.master.web.action.screen.config;

import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.TopicInfo;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerInfoHolder;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import com.tencent.tubemq.server.master.web.common.BrokerQueryResult;
import com.tencent.tubemq.server.master.web.model.BrokerVO;
import com.tencent.tubemq.server.master.web.simplemvc.Action;
import com.tencent.tubemq.server.master.web.simplemvc.RequestContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import javax.servlet.http.HttpServletRequest;


public class BrokerList implements Action {

    private TMaster master;

    public BrokerList(TMaster master) {
        this.master = master;
    }

    @Override
    public void execute(RequestContext context) {
        HttpServletRequest req = context.getReq();
        String strPageNum = req.getParameter("page_num");
        String strPageSize = req.getParameter("page_size");
        //String strTopicName = req.getParameter("topicName");
        //String strConsumeGroup = req.getParameter("consumeGroup");
        int pageNum = TStringUtils.isNotEmpty(strPageNum)
                ? Integer.parseInt(strPageNum) : 1;
        pageNum = pageNum <= 0 ? 1 : pageNum;
        int pageSize = TStringUtils.isNotEmpty(strPageSize)
                ? Integer.parseInt(strPageSize) : 10;
        pageSize = pageSize <= 10 ? 10 : pageSize;
        BrokerInfoHolder infoHolder = master.getBrokerHolder();
        List<BrokerInfo> brokerInfoList = new ArrayList(infoHolder.getBrokerInfoMap().values());
        // *************************************************************************************
        for (int i = 0; i < 95; i++) {
            BrokerInfo info = new BrokerInfo(i, "192.168.0.1", 8123);
            brokerInfoList.add(info);
        }
        // *************************************************************************************

        int totalPage =
                brokerInfoList.size() % pageSize == 0 ? brokerInfoList.size() / pageSize : brokerInfoList
                        .size() / pageSize + 1;
        if (pageNum > totalPage) {
            pageNum = totalPage;
        }
        if (pageNum < 1) {
            pageNum = 1;
        }

        List<BrokerVO> brokerVOList = null;
        if (!brokerInfoList.isEmpty()) {
            Collections.sort(brokerInfoList, new BrokerComparator());
            int fromIndex = pageSize * (pageNum - 1);
            int toIndex =
                    fromIndex + pageSize > brokerInfoList.size() ? brokerInfoList.size() : fromIndex
                            + pageSize;
            List<BrokerInfo> firstPageList = brokerInfoList.subList(fromIndex, toIndex);
            brokerVOList = new ArrayList<BrokerVO>(brokerInfoList.size());
            TopicPSInfoManager psInfoManager = master.getTopicPSInfoManager();
            for (BrokerInfo brokerInfo : firstPageList) {
                BrokerVO brokerVO = new BrokerVO();
                brokerVO.setId(brokerInfo.getBrokerId());
                brokerVO.setIp(brokerInfo.getHost());
                List<TopicInfo> topicInfoList = psInfoManager.getBrokerPubInfoList(brokerInfo);
                brokerVO.setTopicCount(topicInfoList.size());
                int totalPartitionNum = 0;
                for (TopicInfo topicInfo : topicInfoList) {
                    totalPartitionNum += topicInfo.getPartitionNum();
                }
                brokerVO.setPartitionCount(totalPartitionNum);
                brokerVO.setReadable(true);
                brokerVO.setWritable(true);
                brokerVO.setVersion("1.0.0");
                brokerVO.setStatus(1);
                brokerVO.setLastOpTime(new Date());
                brokerVOList.add(brokerVO);
            }
        }
        BrokerQueryResult brokerQueryResult = new BrokerQueryResult();
        brokerQueryResult.setResultList(brokerVOList);
        brokerQueryResult.setCurrentPage(pageNum);
        brokerQueryResult.setTotalPage(totalPage);
        brokerQueryResult.setTotalCount(brokerInfoList.size());
        brokerQueryResult.setPageSize(pageSize);
        context.put("queryResult", brokerQueryResult);
        context.put("page", "brokerList");
    }

    public class BrokerComparator implements Comparator<BrokerInfo> {
        @Override
        public int compare(BrokerInfo o1, BrokerInfo o2) {
            return o1.getBrokerId() - o2.getBrokerId();
        }
    }
}
