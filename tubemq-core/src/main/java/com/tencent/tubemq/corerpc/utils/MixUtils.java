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

package com.tencent.tubemq.corerpc.utils;

import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corerpc.exception.RemoteException;
import java.lang.reflect.Constructor;


public class MixUtils {


    public static Throwable unwrapException(String exceptionMsg) {
        // Perform string to exception conversion processing
        try {
            String[] strExceptionMsgSet =
                    exceptionMsg.split(TokenConstants.SEGMENT_SEP);
            if (strExceptionMsgSet.length > 0) {
                if (!TStringUtils.isBlank(strExceptionMsgSet[0])) {
                    Class clazz = Class.forName(strExceptionMsgSet[0]);
                    if (clazz != null) {
                        Constructor<?> ctor = clazz.getConstructor(String.class);
                        if (ctor != null) {
                            if (strExceptionMsgSet.length == 1) {
                                return (Throwable) ctor.newInstance();
                            } else {
                                if (strExceptionMsgSet[0]
                                        .equalsIgnoreCase("java.lang.NullPointerException")) {
                                    return new NullPointerException("remote return null");
                                } else {
                                    if ((strExceptionMsgSet[1] == null)
                                            || (TStringUtils.isBlank(strExceptionMsgSet[1]))
                                            || (strExceptionMsgSet[1].equalsIgnoreCase("null"))) {
                                        return (Throwable) ctor.newInstance(
                                                "Exception with null StackTrace content");
                                    } else {
                                        return (Throwable) ctor.newInstance(strExceptionMsgSet[1]);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Throwable e) {
            //
        }
        return new RemoteException(exceptionMsg);
    }

}
