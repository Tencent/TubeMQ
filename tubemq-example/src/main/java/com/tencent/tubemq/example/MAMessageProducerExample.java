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

package com.tencent.tubemq.example;

import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.utils.ThreadUtils;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.factory.MessageSessionFactory;
import com.tencent.tubemq.client.factory.TubeMultiSessionFactory;
import com.tencent.tubemq.client.producer.MessageProducer;
import com.tencent.tubemq.client.producer.MessageSentCallback;
import com.tencent.tubemq.client.producer.MessageSentResult;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class MAMessageProducerExample {
    private static final Logger logger =
            LoggerFactory.getLogger(MAMessageProducerExample.class);
    private static final int MAX_PRODUCER_NUM = 100;
    private static final AtomicLong sentSuccCounter = new AtomicLong(0);
    private static final List<MessageProducer> producerList = new ArrayList<MessageProducer>();
    private static int numSessionFactory = 10;
    private static TreeSet<String> topicSet;
    private static int msgCnt;
    private static int producerCnt;
    private static byte[] sendData;
    private final HashMap<MessageProducer, SendRunner> producerMap = new HashMap<MessageProducer, SendRunner>();
    private final ExecutorService sendExecutorService =
            Executors.newFixedThreadPool(MAX_PRODUCER_NUM, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "senderRunner_" + producerMap.size());
                }
            });
    private final String[] arrayKey = {"aaa", "bbb", "ac", "dd", "eee", "fff", "gggg", "hhhh"};
    private AtomicInteger rr = new AtomicInteger(0);
    private TreeSet<String> filters = new TreeSet<String>();
    private int keyCount = 0;
    private int sentCount = 0;
    private ArrayList<MessageSessionFactory> sessionFactoryList = new ArrayList<MessageSessionFactory>();

    public MAMessageProducerExample(final String localHost, final String masterHostAndPort) throws Exception {
        filters.add("aaa");
        filters.add("bbb");
        TubeClientConfig clientConfig = new TubeClientConfig(localHost, masterHostAndPort);
        //clientConfig.setNettyWriteBufferHighWaterMark(1024 * 1024 * 500);
        //clientConfig.setLinkMaxAllowedDelayedMsgCount(80000);
        for (int i = 0; i < numSessionFactory; i++) {
            this.sessionFactoryList.add(new TubeMultiSessionFactory(clientConfig));
        }
    }

    public static void main(String[] args) {
        final String localHost = args[0];
        final String masterHostAndPort = args[1];
        String topics = args[2];
        List<String> topicList = Arrays.asList(topics.split(","));
        topicSet = new TreeSet<String>(topicList);
        msgCnt = Integer.parseInt(args[3]);
        producerCnt = 10;
        if (args.length > 4) {
            producerCnt = Integer.parseInt(args[4]);
        }
        if (producerCnt > MAX_PRODUCER_NUM) {
            producerCnt = MAX_PRODUCER_NUM;
        }

        logger.info("MAMessageProducerExample.main started...");

        String body = "This is a test message from multi-session factory.";
        byte[] bodyData = StringUtils.getBytesUtf8(body);
        int bodyDataLen = bodyData.length;
        int offset = 0;
        final ByteBuffer dataBuffer1 = ByteBuffer.allocate(1024);
        while (dataBuffer1.hasRemaining()) {
            offset = dataBuffer1.arrayOffset();
            if (dataBuffer1.remaining() > bodyDataLen) {
                dataBuffer1.put(bodyData, offset, bodyDataLen);
            } else {
                dataBuffer1.put(bodyData,
                        offset, dataBuffer1.remaining());
            }
        }

        dataBuffer1.flip();
        sendData = dataBuffer1.array();

        try {
            MAMessageProducerExample messageProducer =
                    new MAMessageProducerExample(localHost, masterHostAndPort);

            messageProducer.startService();

            while (sentSuccCounter.get() < msgCnt * producerCnt * topicSet.size()) {
                Thread.sleep(1000);
            }
            messageProducer.getProducerMap().clear();
            messageProducer.shutdown();

        } catch (TubeClientException e) {
            logger.error("TubeClientException: ", e);
        } catch (Throwable e) {
            logger.error("Throwable: ", e);
        }

    }

    public MessageProducer createProducer() throws TubeClientException {
        int index = (rr.incrementAndGet()) % numSessionFactory;
        return sessionFactoryList.get(index).createProducer();
    }

    private void startService() throws TubeClientException {
        for (int i = 0; i < producerCnt; i++) {
            producerList.add(createProducer());
        }

        for (MessageProducer producer : producerList) {
            if (producer != null) {
                producerMap.put(producer, new SendRunner(producer));
                sendExecutorService.submit(producerMap.get(producer));
            }
        }
    }


    public void shutdown() throws Throwable {
        sendExecutorService.shutdownNow();
        for (int i = 0; i < numSessionFactory; i++) {
            sessionFactoryList.get(i).shutdown();
        }

    }

    public HashMap<MessageProducer, SendRunner> getProducerMap() {
        return producerMap;
    }

    public class SendRunner implements Runnable {
        private MessageProducer producer;

        public SendRunner(MessageProducer producer) {
            this.producer = producer;
        }

        public void run() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
            try {
                producer.publish(topicSet);
            } catch (Throwable e2) {
                logger.error("publish exception: ", e2);
            }
            for (int i = 0; i < msgCnt; i++) {
                long millis = System.currentTimeMillis();
                for (String topic : topicSet) {
                    try {
                        Message message = new Message(topic, sendData);
                        message.setAttrKeyVal("index", String.valueOf(1));
                        message.setAttrKeyVal("dataTime", String.valueOf(millis));
                        String keyCode = arrayKey[sentCount++ % arrayKey.length];
                        message.putSystemHeader(keyCode, sdf.format(new Date(millis))); // 定义到分,不能到秒
                        if (filters.contains(keyCode)) {
                            keyCount++;
                        }
                        // 同步发送方式
                        //producer.sendMessage(message);

                        // 异步发送方式，推荐使用
                        producer.sendMessage(message, new DefaultSendCallback());
                    } catch (Throwable e1) {
                        logger.error("sendMessage exception: ", e1);
                    }
                    if (true) {
                        if (i % 5000 == 0) {
                            ThreadUtils.sleep(3000);
                        } else if (i % 4000 == 0) {
                            ThreadUtils.sleep(2000);
                        } else if (i % 2000 == 0) {
                            ThreadUtils.sleep(800);
                        } else if (i % 1000 == 0) {
                            ThreadUtils.sleep(400);
                        }
                    }
                }
            }
            try {
                producer.shutdown();
            } catch (Throwable e) {
                logger.error("producer shutdown error: ", e);
            }

        }
    }

    final class DefaultSendCallback implements MessageSentCallback {

        public void onMessageSent(MessageSentResult result) {
            if (result.isSuccess()) {
                if (sentSuccCounter.incrementAndGet() % 1000 == 0) {
                    logger.info("Send " + sentSuccCounter.get() + " message, keyCount is " + keyCount);
                }
            } else {
                if (result.getErrCode() != TErrCodeConstants.SERVER_RECEIVE_OVERFLOW) {
                    logger.error("Send message failed!" + result.getErrMsg());
                }
            }
        }

        public void onException(Throwable e) {
            logger.error("Send message error!", e);
        }
    }
}
