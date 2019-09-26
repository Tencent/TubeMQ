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


public final class MessagePullSetConsumerExample {

    private static final Logger logger =
            LoggerFactory.getLogger(MessagePullSetConsumerExample.class);
    private static final AtomicLong counter = new AtomicLong(0);
    private final String masterHostAndPort;
    private final String group;
    private final String localHost;
    private PullMessageConsumer messagePullConsumer;
    private MessageSessionFactory messageSessionFactory;


    public MessagePullSetConsumerExample(String localHost, String masterHostAndPort, String group) throws Exception {
        this.masterHostAndPort = masterHostAndPort;
        this.group = group;
        this.localHost = localHost;
        ConsumerConfig consumerConfig =
                new ConsumerConfig(this.localHost, this.masterHostAndPort, this.group);
        this.messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        this.messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
    }

    public static void main(String[] args) {
        final String localhost = args[0];
        final String masterHostAndPort = args[1];

        final String topics = args[2];
        final List<String> topicList = Arrays.asList(topics.split(","));
        final String group = args[3];

        final int consumeCount = Integer.parseInt(args[4]);
        final Map<String, Long> partOffsetMap = new ConcurrentHashMap<String, Long>();
        partOffsetMap.put("123:test_1:0", 0L);
        partOffsetMap.put("123:test_1:1", 0L);
        partOffsetMap.put("123:test_1:2", 0L);
        partOffsetMap.put("123:test_2:0", 350L);
        partOffsetMap.put("123:test_2:1", 350L);
        partOffsetMap.put("123:test_2:2", 350L);

        ExecutorService executorService = Executors.newCachedThreadPool();

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    int getCount = consumeCount;
                    MessagePullSetConsumerExample messageConsumer =
                            new MessagePullSetConsumerExample(localhost, masterHostAndPort, group);
                    messageConsumer.subscribe(topicList, partOffsetMap);
                    // 检查 当前是否有分配到分区，有分配到的话就继续
                    Map<String, ConsumeOffsetInfo> curPartsMap = null;
                    while (curPartsMap == null || curPartsMap.isEmpty()) {
                        ThreadUtils.sleep(1000);
                        curPartsMap = messageConsumer.getCurrPartitionOffsetMap();
                    }
                    logger.info("Get allocated partitions are " + curPartsMap.toString());
                    // 进行实际的消费处理
                    do {
                        ConsumerResult result = messageConsumer.getMessage();
                        if (result.isSuccess()) {
                            List<Message> messageList = result.getMessageList();
                            if (messageList != null) {
                                logger.info("Receive messages:" + counter.addAndGet(messageList.size()));
                            }
                            // GetMessage返回的Offset表示是这个请求提取到的数据的初始位置
                            // 如果消费组是纯粹的Pull模式，起始拉取位置可以保存，如果不是，就只能取confirmConsume的返回 才行
                            long oldValue =
                                    partOffsetMap.get(
                                            result.getPartitionKey()) == null
                                            ? -1
                                            : partOffsetMap.get(result.getPartitionKey()).longValue();
                            partOffsetMap.put(result.getPartitionKey(), result.getCurrOffset());
                            logger.info("GetMessage , partitionKey="
                                    + result.getPartitionKey() + ", oldValue="
                                    + oldValue + ", newVal=" + result.getCurrOffset());
                            // 这里是取confirm的结果 Offset进行保存
                            ConsumerResult confirmResult =
                                    messageConsumer.confirmConsume(result.getConfirmContext(), true);
                            if (confirmResult.isSuccess()) {
                                oldValue =
                                        partOffsetMap.get(
                                                result.getPartitionKey()) == null
                                                ? -1
                                                : partOffsetMap.get(result.getPartitionKey()).longValue();
                                partOffsetMap.put(result.getPartitionKey(), confirmResult.getCurrOffset());
                                logger.info("ConfirmConsume , partitionKey="
                                        + confirmResult.getPartitionKey() + ", oldValue="
                                        + oldValue + ", newVal=" + confirmResult.getCurrOffset());
                            } else {
                                logger.info("ConfirmConsume failure, errCode is "
                                        + confirmResult.getErrCode() + ",errInfo is " + confirmResult.getErrMsg());
                            }
                        } else {
                            if (result.getErrCode() == 400) {
                                ThreadUtils.sleep(400);
                            } else {
                                if (result.getErrCode() != 404) {
                                    logger.info("Receive messages errorCode is "
                                            + result.getErrCode() + ", Error message is " + result.getErrMsg());
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

    public void subscribe(List<String> topicList,
                          Map<String, Long> partOffsetMap) throws TubeClientException {
        TreeSet<String> filters = new TreeSet<String>();
        filters.add("aaa");
        filters.add("bbb");
        for (String topic : topicList) {
            this.messagePullConsumer.subscribe(topic, filters);
        }
        String sessionKey = "test_reset2";
        int consumerCount = 2;
        boolean isSelectBig = false;
        messagePullConsumer.completeSubscribe(sessionKey,
                consumerCount, isSelectBig, partOffsetMap);
    }

    public ConsumerResult getMessage() throws TubeClientException {
        return messagePullConsumer.getMessage();
    }

    public ConsumerResult confirmConsume(final String confirmContext,
                                         boolean isConsumed) throws TubeClientException {
        return messagePullConsumer.confirmConsume(confirmContext, isConsumed);
    }

    public Map<String, ConsumeOffsetInfo> getCurrPartitionOffsetMap() throws TubeClientException {
        return messagePullConsumer.getCurConsumedPartitions();
    }
}


