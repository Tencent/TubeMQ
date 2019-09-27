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

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class WebParameterTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void validParameterTest() throws Exception {
        long value = WebParameterUtils.validLongDataParameter("test", "100",
                true, 50);
        Assert.assertEquals(value, 100);

        value = WebParameterUtils.validLongDataParameter("test", null,
                false, 50);
        Assert.assertEquals(value, 50);

        thrown.expect(Exception.class);
        thrown.expectMessage("Null or blank value of test parameter");
        WebParameterUtils.validLongDataParameter("test", "",
                true, 50);

        thrown.expect(Exception.class);
        thrown.expectMessage("the value of test parameter must only contain numbers");
        WebParameterUtils.validLongDataParameter("test", "100.0",
                true, 50);

        DateFormat sdf = new SimpleDateFormat(TBaseConstants.META_TMP_DATE_VALUE);
        Date date = WebParameterUtils.validDateParameter("test",
                "20190101012259", 20, true, new Date());
        Assert.assertEquals(date, sdf.parse("20190101012259"));

        String ip = WebParameterUtils.validAddressParameter("testIp",
                "10.1.1.1", 20, true, "10.0.0.0");
        Assert.assertEquals("10.1.1.1", ip);

        ip = WebParameterUtils.validAddressParameter("testIp",
                "", 20, false, "10.0.0.0");
        Assert.assertEquals("10.0.0.0", ip);
    }
}
