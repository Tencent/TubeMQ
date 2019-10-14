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

import com.tencent.tubemq.client.config.ConsumerConfig;
import com.tencent.tubemq.client.consumer.ConsumeOffsetInfo;
import com.tencent.tubemq.client.consumer.ConsumerResult;
import com.tencent.tubemq.client.consumer.PullMessageConsumer;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.factory.MessageSessionFactory;
import com.tencent.tubemq.client.factory.TubeSingleSessionFactory;
import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.utils.ThreadUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to reset offset on consuming. The main difference from {@link MessagePullConsumerExample}
 * is that we call {@link PullMessageConsumer#completeSubscribe(String, int, boolean, Map)} instead of
 * {@link PullMessageConsumer#completeSubscribe()}. The former supports multiple options to configure
 * when to reset offset.
 */
public final class MessagePullSetConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(MessagePullSetConsumerExample.class);
    private static final AtomicLong counter = new AtomicLong(0);

    private final PullMessageConsumer messagePullConsumer;
    private final MessageSessionFactory messageSessionFactory;

    public MessagePullSetConsumerExample(
        String localHost,
        String masterHostAndPort,
        String group
    ) throws Exception {
        ConsumerConfig consumerConfig = new ConsumerConfig(localHost, masterHostAndPort, group);
        this.messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        this.messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
    }

    public static void main(String[] args) {
        final String localhost = args[0];
        final String masterHostAndPort = args[1];
        final String topics = args[2];
        final String group = args[3];
        final int consumeCount = Integer.parseInt(args[4]);
        final Map<String, Long> partOffsetMap = new ConcurrentHashMap<>();
        partOffsetMap.put("123:test_1:0", 0L);
        partOffsetMap.put("123:test_1:1", 0L);
        partOffsetMap.put("123:test_1:2", 0L);
        partOffsetMap.put("123:test_2:0", 350L);
        partOffsetMap.put("123:test_2:1", 350L);
        partOffsetMap.put("123:test_2:2", 350L);

        final List<String> topicList = Arrays.asList(topics.split(","));

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    int getCount = consumeCount;
                    MessagePullSetConsumerExample messageConsumer =
                        new MessagePullSetConsumerExample(localhost, masterHostAndPort, group);
                    messageConsumer.subscribe(topicList, partOffsetMap);

                    // wait until the consumer is allocated parts
                    Map<String, ConsumeOffsetInfo> curPartsMap = null;
                    while (curPartsMap == null || curPartsMap.isEmpty()) {
                        ThreadUtils.sleep(1000);
                        curPartsMap = messageConsumer.getCurrPartitionOffsetMap();
                    }

                    logger.info("Get allocated partitions are " + curPartsMap.toString());

                    // main logic of consuming
                    do {
                        ConsumerResult result = messageConsumer.getMessage();
                        if (result.isSuccess()) {
                            List<Message> messageList = result.getMessageList();
                            if (messageList != null) {
                                logger.info("Receive messages:" + counter.addAndGet(messageList.size()));
                            }

                            // Offset returned by GetMessage represents the initial offset of this request
                            // if consumer group is pure Pull mode, the initial offset can be saved;
                            // if not, we have to use the return value of confirmConsume
                            long oldValue = partOffsetMap.get(
                                result.getPartitionKey()) == null
                                ? -1
                                : partOffsetMap.get(result.getPartitionKey());
                            partOffsetMap.put(result.getPartitionKey(), result.getCurrOffset());
                            logger.info(
                                "GetMessage , partitionKey={}, oldValue={}, newVal={}",
                                new Object[]{result.getPartitionKey(), oldValue, result.getCurrOffset()});

                            // save the Offset from the return value of confirmConsume
                            ConsumerResult confirmResult = messageConsumer.confirmConsume(
                                result.getConfirmContext(),
                                true);
                            if (confirmResult.isSuccess()) {
                                oldValue = partOffsetMap.get(
                                    result.getPartitionKey()) == null
                                    ? -1
                                    : partOffsetMap.get(result.getPartitionKey());
                                partOffsetMap.put(result.getPartitionKey(), confirmResult.getCurrOffset());
                                logger.info(
                                    "ConfirmConsume , partitionKey={}, oldValue={}, newVal={}",
                                    new Object[]{
                                        confirmResult.getPartitionKey(),
                                        oldValue,
                                        confirmResult.getCurrOffset()});
                            } else {
                                logger.info(
                                    "ConfirmConsume failure, errCode is {}, errInfo is {}.",
                                    confirmResult.getErrCode(),
                                    confirmResult.getErrMsg());
                            }
                        } else {
                            if (result.getErrCode() == 400) {
                                ThreadUtils.sleep(400);
                            } else {
                                if (result.getErrCode() != 404) {
                                    logger.info(
                                        "Receive messages errorCode is {}, Error message is {}",
                                        result.getErrCode(),
                                        result.getErrMsg());
                                }
                            }
                        }
                        if (consumeCount >= 0) {
                            if (--getCount <= 0) {
                                break;
                            }
                        }
                    } while (true);
                } catch (Exception e) {
                    logger.error("Create consumer failed!", e);
                }
            }
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(60 * 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Thread Pool shutdown has been interrupted!");
        }
    }

    public void subscribe(
        List<String> topicList,
        Map<String, Long> partOffsetMap
    ) throws TubeClientException {
        TreeSet<String> filters = new TreeSet<>();
        filters.add("aaa");
        filters.add("bbb");
        for (String topic : topicList) {
            this.messagePullConsumer.subscribe(topic, filters);
        }
        String sessionKey = "test_reset2";
        int consumerCount = 2;
        boolean isSelectBig = false;
        messagePullConsumer.completeSubscribe(
            sessionKey,
            consumerCount,
            isSelectBig,
            partOffsetMap);
    }

    public ConsumerResult getMessage() throws TubeClientException {
        return messagePullConsumer.getMessage();
    }

    public ConsumerResult confirmConsume(
        String confirmContext,
        boolean isConsumed
    ) throws TubeClientException {
        return messagePullConsumer.confirmConsume(confirmContext, isConsumed);
    }

    public Map<String, ConsumeOffsetInfo> getCurrPartitionOffsetMap() throws TubeClientException {
        return messagePullConsumer.getCurConsumedPartitions();
    }
}


