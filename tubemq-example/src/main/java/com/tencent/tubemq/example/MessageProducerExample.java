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

import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.factory.MessageSessionFactory;
import com.tencent.tubemq.client.factory.TubeSingleSessionFactory;
import com.tencent.tubemq.client.producer.MessageProducer;
import com.tencent.tubemq.client.producer.MessageSentCallback;
import com.tencent.tubemq.client.producer.MessageSentResult;
import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.utils.ThreadUtils;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class MessageProducerExample {

    private static final Logger logger =
            LoggerFactory.getLogger(MessageProducerExample.class);
    private static final ConcurrentHashMap<String, AtomicLong> counterMap = new ConcurrentHashMap<String, AtomicLong>();
    String[] arrayKey = {"aaa", "bbb", "ac", "dd", "eee", "fff", "gggg", "hhhh"};
    private MessageProducer messageProducer;
    private TreeSet<String> filters = new TreeSet<String>();
    private int keyCount = 0;
    private int sentCount = 0;
    private MessageSessionFactory messageSessionFactory;

    public MessageProducerExample(final String localHost, final String masterHostAndPort) throws Exception {
        filters.add("aaa");
        filters.add("bbb");
        TubeClientConfig clientConfig = new TubeClientConfig(localHost, masterHostAndPort);
        //clientConfig.setNettyWriteBufferHighWaterMark(1024 * 1024 * 500);
        //clientConfig.setLinkMaxAllowedDelayedMsgCount(80000);
        this.messageSessionFactory = new TubeSingleSessionFactory(clientConfig);
        this.messageProducer = this.messageSessionFactory.createProducer();
    }

    public static void main(String[] args) {
        final String localHost = args[0];
        final String masterHostAndPort = args[1];
        String topics = args[2];
        List<String> topicList = Arrays.asList(topics.split(","));
        int cnt = Integer.parseInt(args[3]);

        String body = "This is a test message from single-session-factory.";
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
        final byte[] sentData = dataBuffer1.array();
        long currtime = System.currentTimeMillis();
        try {
            MessageProducerExample messageProducer =
                    new MessageProducerExample(localHost, masterHostAndPort);
            messageProducer.publishTopics(topicList);
            for (int i = 0; i < cnt; i++) {
                for (String topic : topicList) {
                    try {
                        // 同步发送方式
                        // messageProducer.sendMessage(topic, body.getBytes());

                        // 异步发送方式，推荐使用
                        messageProducer.sendMessageAsync(i, currtime, topic, sentData,
                                messageProducer.new DefaultSendCallback());
                    } catch (Throwable e1) {
                        logger.error("Send Message throw exception  ", e1);
                    }
                }
                //if(i%5 == 0) {
                if (true) {
                    if (i % 20000 == 0) {
                        ThreadUtils.sleep(4000);
                    } else if (i % 10000 == 0) {
                        ThreadUtils.sleep(2000);
                    } else if (i % 2500 == 0) {
                        ThreadUtils.sleep(300);
                    }
                }
            }
            ThreadUtils.sleep(20000);
            for (Map.Entry<String, AtomicLong> entry : counterMap.entrySet()) {
                logger.info("********* Current " + entry.getKey()
                        + " Message sent count is " + entry.getValue().get());
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }

    public void publishTopics(List<String> topicList) throws TubeClientException {
        this.messageProducer.publish(new TreeSet<String>(topicList));
    }

    /**
     * 使用同步方式发送消息
     */
    public void sendMessage(String topic, byte[] body) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        long currTimeMillis = System.currentTimeMillis();
        Message message = new Message(topic, body);
        message.setAttrKeyVal("index", String.valueOf(1));
        message.setAttrKeyVal("dataTime", String.valueOf(currTimeMillis));
        message.putSystemHeader("test", sdf.format(new Date(currTimeMillis))); // 定义到分,不能到秒
        try {
            MessageSentResult result =
                    messageProducer.sendMessage(message);
            if (result.isSuccess()) {
                //
            } else {
                logger.error("Send message failed!" + result.getErrMsg());
            }

        } catch (TubeClientException e) {
            logger.error("Send message failed!", e);
        } catch (InterruptedException e) {
            logger.error("Send message failed!", e);
        }
    }

    /**
     * 使用异步方式发送消息 该方式发送效率比较高，推荐使用
     */
    public void sendMessageAsync(int id, long currtime,
                                 String topic, byte[] body,
                                 MessageSentCallback callback) {
        Message message = new Message(topic, body);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        long currTimeMillis = System.currentTimeMillis();
        message.setAttrKeyVal("index", String.valueOf(1));
        String keyCode = arrayKey[sentCount++ % arrayKey.length];
        message.putSystemHeader(keyCode, sdf.format(new Date(currTimeMillis))); // 定义到分,不能到秒
        if (filters.contains(keyCode)) {
            keyCount++;
        }
        try {
            message.setAttrKeyVal("dataTime", String.valueOf(currTimeMillis));
            messageProducer.sendMessage(message, callback);
        } catch (TubeClientException e) {
            logger.error("Send message failed!", e);
        } catch (InterruptedException e) {
            logger.error("Send message failed!", e);
        }
    }

    final class DefaultSendCallback implements MessageSentCallback {

        public void onMessageSent(MessageSentResult result) {
            if (result.isSuccess()) {
                String topicName = result.getMessage().getTopic();
                AtomicLong currCount = counterMap.get(topicName);
                if (currCount == null) {
                    AtomicLong tmpCount = new AtomicLong(0);
                    currCount = counterMap.putIfAbsent(topicName, tmpCount);
                    if (currCount == null) {
                        currCount = tmpCount;
                    }
                }
                if (currCount.incrementAndGet() % 1000 == 0) {
                    logger.info("Send " + topicName + " " + currCount.get() + " message, keyCount is " + keyCount);
                }

            } else {
                logger.error("Send message failed!" + result.getErrMsg());
            }
        }

        public void onException(Throwable e) {
            logger.error("Send message error!", e);
        }
    }

}
