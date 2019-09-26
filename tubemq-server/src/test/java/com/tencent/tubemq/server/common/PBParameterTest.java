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
package com.tencent.tubemq.server.common;

import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.server.common.paramcheck.PBParameterUtils;
import com.tencent.tubemq.server.common.paramcheck.ParamCheckResult;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class PBParameterTest {
    @Test
    public void checkProducerTopicTest() {
        ParamCheckResult result = PBParameterUtils.checkProducerTopicList(null, null);
        Assert.assertEquals(result.errCode, TErrCodeConstants.BAD_REQUEST);
        final List<String> topicList = new ArrayList<>();
        topicList.add("test1");
        result = PBParameterUtils.checkProducerTopicList(topicList, new StringBuilder(128));
        Assert.assertEquals(result.errCode, TErrCodeConstants.SUCCESS);
        for (int i = 0; i < 1025; i++) {
            topicList.add("test" + i);
        }
        result = PBParameterUtils.checkProducerTopicList(topicList, new StringBuilder(128));
        Assert.assertEquals(result.errCode, TErrCodeConstants.BAD_REQUEST);
    }

    @Test
    public void checkConsumerTopicTest() {
        ParamCheckResult result = PBParameterUtils.checkConsumerTopicList(null, null);
        Assert.assertEquals(result.errCode, TErrCodeConstants.BAD_REQUEST);
        final List<String> topicList = new ArrayList<>();
        topicList.add("test1");
        result = PBParameterUtils.checkConsumerTopicList(topicList, new StringBuilder(128));
        Assert.assertEquals(result.errCode, TErrCodeConstants.SUCCESS);
        for (int i = 0; i < 1025; i++) {
            topicList.add("test" + i);
        }
        result = PBParameterUtils.checkConsumerTopicList(topicList, new StringBuilder(128));
        Assert.assertEquals(result.errCode, TErrCodeConstants.BAD_REQUEST);
    }

    @Test
    public void checkIdTest() {
        ParamCheckResult result = PBParameterUtils.checkClientId("100", new StringBuilder(128));
        Assert.assertEquals(result.errCode, TErrCodeConstants.SUCCESS);
        result = PBParameterUtils.checkClientId("", new StringBuilder(128));
        Assert.assertEquals(result.errCode, TErrCodeConstants.BAD_REQUEST);
        result = PBParameterUtils.checkBrokerId("100", new StringBuilder(128));
        Assert.assertEquals(result.errCode, TErrCodeConstants.SUCCESS);
        result = PBParameterUtils.checkBrokerId("", new StringBuilder(128));
        Assert.assertEquals(result.errCode, TErrCodeConstants.BAD_REQUEST);
    }
}
