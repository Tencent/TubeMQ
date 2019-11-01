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

package com.tencent.tubemq.corerpc.protocol;

import com.tencent.tubemq.corebase.utils.ServiceStatusHolder;
import com.tencent.tubemq.corerpc.RequestWrapper;
import com.tencent.tubemq.corerpc.ResponseWrapper;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.codec.PbEnDecoder;
import com.tencent.tubemq.corerpc.exception.ServiceStoppingException;
import com.tencent.tubemq.corerpc.exception.StandbyException;
import com.tencent.tubemq.corerpc.server.RequestContext;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RpcProtocol implements Protocol {

    public static final int RPC_PROTOCOL_TCP = 10;
    public static final int RPC_PROTOCOL_TLS = 11;
    public static final int RPC_PROTOCOL_VERSION = 1;

    private static final Logger logger =
            LoggerFactory.getLogger(RpcProtocol.class);
    private final Map<Integer, Object> processors =
            new HashMap<Integer, Object>();
    private final Map<Integer, Method> cacheMethods =
            new HashMap<Integer, Method>();
    private final Map<Integer, ExecutorService> threadPools =
            new HashMap<Integer, ExecutorService>();
    private boolean isOverTLS = false;

    @Override
    public void registerService(boolean isOverTLS, String serviceName,
                                Object instance, ExecutorService threadPool) throws Exception {
        this.isOverTLS = isOverTLS;
        int serviceId = PbEnDecoder.getServiceIdByServiceName(serviceName);
        processors.put(serviceId, instance);
        threadPools.put(serviceId, threadPool);
        Class<?> instanceClass = instance.getClass();
        Method[] methods = instanceClass.getMethods();
        for (Method method : methods) {
            try {
                String methodName = method.getName();
                int methodId = PbEnDecoder.getMethIdByName(methodName);
                cacheMethods.put(methodId, method);
            } catch (Throwable e) {
                //
            }
        }
    }

    @Override
    public void removeService(String serviceName) throws Exception {
        int serviceId = PbEnDecoder.getServiceIdByServiceName(serviceName);
        Object instance = processors.remove(serviceId);
        Class<?> instanceClass = instance.getClass();
        Method[] methods = instanceClass.getMethods();
        for (Method method : methods) {
            try {
                String methodName = method.getName();
                int methodId = PbEnDecoder.getMethIdByName(methodName);
                cacheMethods.remove(methodId);
            } catch (Throwable e) {
                //
            }
        }
    }

    @Override
    public void removeAllService() {
        processors.clear();
        cacheMethods.clear();
        for (ExecutorService executorService : threadPools.values()) {
            if (executorService != null) {
                executorService.shutdown();
                try {
                    while (!executorService.awaitTermination(3, TimeUnit.SECONDS)) {
                        logger.warn("threadpool not stop yet,try again");
                    }
                    logger.warn("threadpool stop success...");
                } catch (InterruptedException e) {
                    logger.warn("threadpool stop has been InterruptedException...");
                }
            }
        }
    }

    @Override
    public void handleRequest(final RequestContext context, final String rmtAddress) throws Exception {
        ResponseWrapper responseWrapper = null;
        RequestWrapper requestWrapper = context.getRequest();
        if (System.currentTimeMillis() - context.getReceiveTime() > requestWrapper.getTimeout()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Timeout when request arrived, so give up processing this request from : {}",
                       rmtAddress);
            }
            return;
        }
        if (ServiceStatusHolder.isServiceStopped()) {
            context.write(new ResponseWrapper(RpcConstants.RPC_FLAG_MSG_TYPE_RESPONSE,
                    requestWrapper.getSerialNo(), requestWrapper.getServiceType(),
                    RPC_PROTOCOL_VERSION, new ServiceStoppingException("service is stopping...")));
        }
        Method method = null;
        StringBuilder sBuilder = new StringBuilder(512);
        try {
            if (!PbEnDecoder.isValidServiceTypeAndMethod(requestWrapper.getServiceType(),
                    requestWrapper.getMethodId(), sBuilder)) {
                throw new Exception(sBuilder.toString());
            }
            Object processor = processors.get(requestWrapper.getServiceType());
            if (processor == null) {
                throw new Exception(sBuilder.append("No service ")
                        .append(requestWrapper.getServiceType())
                        .append(" found on the server").toString());
            }
            method = cacheMethods.get(requestWrapper.getMethodId());
            if (method == null) {
                throw new Exception(sBuilder.append("No method ")
                        .append(requestWrapper.getMethodId())
                        .append(" in service ")
                        .append(requestWrapper.getServiceType())
                        .append(" found on the server").toString());
            }
            Object result =
                    method.invoke(processor, requestWrapper.getRequestData(), rmtAddress, isOverTLS);
            responseWrapper =
                    new ResponseWrapper(RpcConstants.RPC_FLAG_MSG_TYPE_RESPONSE,
                            requestWrapper.getSerialNo(), requestWrapper.getServiceType(),
                            RPC_PROTOCOL_VERSION, requestWrapper.getMethodId(), result);
        } catch (Throwable e2) {
            String errorClass = null;
            String errorInfo = null;
            if (e2.getCause() != null && e2.getCause() instanceof StandbyException) {
                errorClass = e2.getCause().getClass().getName();
                errorInfo = e2.getCause().getMessage();
            } else {
                errorClass = e2.getClass().getName();
                errorInfo = e2.getMessage();
            }
            responseWrapper =
                    new ResponseWrapper(RpcConstants.RPC_FLAG_MSG_TYPE_RESPONSE,
                            requestWrapper.getSerialNo(), requestWrapper.getServiceType(),
                            RPC_PROTOCOL_VERSION, errorClass, errorInfo);
        }
        try {
            context.write(responseWrapper);
        } catch (Exception e) {
            logger.error("Write response error!", e);
        }
    }


}
