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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to produce message normally.
 *
 * <p>Producer supports publish one or more topics via {@link MessageProducer#publish(String)}
 * or {@link MessageProducer#publish(Set)}. Note that topic publish asynchronously.
 */
public final class MessageProducerExample {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducerExample.class);
    private static final ConcurrentHashMap<String, AtomicLong> counterMap = new ConcurrentHashMap<>();

    private final String[] arrayKey = {"aaa", "bbb", "ac", "dd", "eee", "fff", "gggg", "hhhh"};
    private final Set<String> filters = new TreeSet<>();
    private final MessageProducer messageProducer;
    private final MessageSessionFactory messageSessionFactory;

    private int keyCount = 0;
    private int sentCount = 0;

    public MessageProducerExample(String localHost, String masterHostAndPort) throws Exception {
        filters.add("aaa");
        filters.add("bbb");

        TubeClientConfig clientConfig = new TubeClientConfig(localHost, masterHostAndPort);
        this.messageSessionFactory = new TubeSingleSessionFactory(clientConfig);
        this.messageProducer = messageSessionFactory.createProducer();
    }

    public static void main(String[] args) {
        final String localHost = args[0];
        final String masterHostAndPort = args[1];
        final String topics = args[2];
        final List<String> topicList = Arrays.asList(topics.split(","));
        final int cnt = Integer.parseInt(args[3]);

        String body = "This is a test message from single-session-factory.";
        byte[] bodyData = StringUtils.getBytesUtf8(body);
        int bodyDataLen = bodyData.length;
        final ByteBuffer dataBuffer1 = ByteBuffer.allocate(1024);
        while (dataBuffer1.hasRemaining()) {
            int offset = dataBuffer1.arrayOffset();
            dataBuffer1.put(bodyData, offset, Math.min(dataBuffer1.remaining(), bodyDataLen));
        }
        dataBuffer1.flip();
        try {
            MessageProducerExample messageProducer = new MessageProducerExample(localHost, masterHostAndPort);
            messageProducer.publishTopics(topicList);
            for (int i = 0; i < cnt; i++) {
                for (String topic : topicList) {
                    try {
                        // next line sends message synchronously, which is not recommended
                        // messageProducer.sendMessage(topic, body.getBytes());

                        // send message asynchronously, recommended
                        messageProducer.sendMessageAsync(
                            i,
                            topic,
                            dataBuffer1.array(),
                            messageProducer.new DefaultSendCallback());
                    } catch (Throwable e1) {
                        logger.error("Send Message throw exception  ", e1);
                    }
                }

                if (i % 20000 == 0) {
                    ThreadUtils.sleep(4000);
                } else if (i % 10000 == 0) {
                    ThreadUtils.sleep(2000);
                } else if (i % 2500 == 0) {
                    ThreadUtils.sleep(300);
                }
            }
            ThreadUtils.sleep(20000);
            for (Map.Entry<String, AtomicLong> entry : counterMap.entrySet()) {
                logger.info(
                    "********* Current {} Message sent count is {}",
                    entry.getKey(),
                    entry.getValue().get());
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }

    public void publishTopics(List<String> topicList) throws TubeClientException {
        this.messageProducer.publish(new TreeSet<>(topicList));
    }

    /**
     * Send message synchronous.
     */
    public void sendMessage(String topic, byte[] body) {
        // date format is accurate to minute, not to second
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        long currTimeMillis = System.currentTimeMillis();
        Message message = new Message(topic, body);
        message.setAttrKeyVal("index", String.valueOf(1));
        message.setAttrKeyVal("dataTime", String.valueOf(currTimeMillis));
        message.putSystemHeader("test", sdf.format(new Date(currTimeMillis)));
        try {
            MessageSentResult result = messageProducer.sendMessage(message);
            if (!result.isSuccess()) {
                logger.error("Send message failed!" + result.getErrMsg());
            }
        } catch (TubeClientException | InterruptedException e) {
            logger.error("Send message failed!", e);
        }
    }

    /**
     * Send message synchronous. More efficient and recommended.
     */
    public void sendMessageAsync(
        int id,
        String topic,
        byte[] body,
        MessageSentCallback callback
    ) {
        Message message = new Message(topic, body);

        // date format is accurate to minute, not to second
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        long currTimeMillis = System.currentTimeMillis();
        message.setAttrKeyVal("index", String.valueOf(1));
        String keyCode = arrayKey[sentCount++ % arrayKey.length];
        message.putSystemHeader(keyCode, sdf.format(new Date(currTimeMillis)));
        if (filters.contains(keyCode)) {
            keyCount++;
        }
        try {
            message.setAttrKeyVal("dataTime", String.valueOf(currTimeMillis));
            messageProducer.sendMessage(message, callback);
        } catch (TubeClientException | InterruptedException e) {
            logger.error("Send message failed!", e);
        }
    }

    private class DefaultSendCallback implements MessageSentCallback {

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
