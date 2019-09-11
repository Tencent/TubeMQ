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

package com.tencent.tubemq.server.broker.exception;

/***
 * Save offset occur error throw this Exception.
 */
public class OffsetStoreException extends Exception {
    // Offset存储异常包装类，
    // 在存储数据到zk时的异常封装
    static final long serialVersionUID = -1L;

    public OffsetStoreException() {
        super();

    }

    public OffsetStoreException(String message, Throwable cause) {
        super(message, cause);

    }

    public OffsetStoreException(String message) {
        super(message);

    }

    public OffsetStoreException(Throwable cause) {
        super(cause);

    }

}
