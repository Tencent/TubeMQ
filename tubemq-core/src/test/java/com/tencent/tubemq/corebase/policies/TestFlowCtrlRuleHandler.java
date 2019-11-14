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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.Test;

public class TestFlowCtrlRuleHandler {

    private static String mockFlowCtrlInfo() {
        // 0: current limit, 1: frequency limit, 2: SSD transfer 3: request frequency control
        String str = "[{\"type\":0,\"rule\":[{\"start\":\"08:00\",\"end\":\"17:59\",\"dltInM\":1024,"
                + "\"limitInM\":20,\"freqInMs\":1000},{\"start\":\"18:00\",\"end\":\"22:00\","
                + "\"dltInM\":1024,\"limitInM\":20,\"freqInMs\":5000}]},{\"type\":2,\"rule\""
                + ":[{\"start\":\"12:00\",\"end\":\"23:59\",\"dltStInM\":20480,\"dltEdInM\":2048}]}"
                + ",{\"type\":1,\"rule\":[{\"zeroCnt\":3,\"freqInMs\":300},{\"zeroCnt\":8,\"freqInMs\""
                + ":1000}]},{\"type\":3,\"rule\":[{\"normFreqInMs\":0,\"filterFreqInMs\":100,"
                + "\"minDataFilterFreqInMs\":400}]}]";
        return str;
    }

    @Test
    public void testFlowCtrlRuleHandler() {
        try {
            FlowCtrlRuleHandler handler = new FlowCtrlRuleHandler(true);
            handler.updateDefFlowCtrlInfo(1001, 2, 10, mockFlowCtrlInfo());
            TimeZone timeZone = TimeZone.getTimeZone("GMT+8:00");
            Calendar rightNow = Calendar.getInstance(timeZone);
            int hour = rightNow.get(Calendar.HOUR_OF_DAY);
            int minu = rightNow.get(Calendar.MINUTE);
            int curTime = hour * 100 + minu;

            // current data limit test
            FlowCtrlResult result = handler.getCurDataLimit(2000);
            if (curTime >= 800 && curTime <= 1759) {
                assertTrue(result.dataLtInSize == 20 * 1024 * 1024L);
                assertTrue(result.freqLtInMs == 1000L);
            } else if (curTime >= 1800 && curTime < 2200) {
                assertTrue(result.dataLtInSize == 20 * 1024 * 1024L);
                assertTrue(result.freqLtInMs == 5000L);
            } else {
                assertEquals("result should be null", result, null);
            }
            result = handler.getCurDataLimit(1000);
            assertEquals("result should be null", result, null);

            //  request frequency control
            FlowCtrlItem item = handler.getFilterCtrlItem();
            assertEquals(item.getDataLtInSZ(), 0);
            assertEquals(item.getZeroCnt(), 400);
            assertEquals(item.getFreqLtInMs(), 100);

            // check ssd ctrl
            SSDCtrlResult ssdResult = handler.getCurSSDStartDltInSZ();
            if (curTime >= 1200) {
                assertEquals(ssdResult.dataEndDLtInSz, 2048L * 1024L * 1024L);
                assertEquals(ssdResult.dataStartDltInSize, 20480L * 1024L * 1024L);
            } else {
                assertEquals(ssdResult.dataEndDLtInSz, 0L);
                assertEquals(ssdResult.dataStartDltInSize, Long.MAX_VALUE);
            }

            //check values
            assertEquals(handler.getNormFreqInMs(), 100);
            assertEquals(handler.getFlowCtrlId(), 10);
            assertEquals(handler.getMinDataFreqInMs(), 400);
            assertEquals(handler.getMinZeroCnt(), 3);
            assertEquals(handler.getQryPriorityId(), 2);
            assertEquals(handler.getSsdTranslateId(), 1001);
            assertEquals(handler.getFlowCtrlId(), 10);

            System.out.println();
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
