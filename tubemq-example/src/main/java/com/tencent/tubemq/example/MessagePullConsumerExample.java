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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to consume message by pull.
 *
 * <p>Consume message in pull mode achieved by {@link PullMessageConsumer#getMessage()}.
 * Note that whenever {@link PullMessageConsumer#getMessage()} returns successfully, the
 * return value(whether or not to be {@code null}) should be processed by
 * {@link PullMessageConsumer#confirmConsume(String, boolean)}.
 */
public final class MessagePullConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(MessagePullConsumerExample.class);
    private static final MsgRecvStats msgRecvStats = new MsgRecvStats();

    private final PullMessageConsumer messagePullConsumer;
    private final MessageSessionFactory messageSessionFactory;

    public MessagePullConsumerExample(
        String localHost,
        String masterHostAndPort,
        String group
    ) throws Exception {
        ConsumerConfig consumerConfig = new ConsumerConfig(localHost, masterHostAndPort, group);
        consumerConfig.setConsumeModel(0);
        this.messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        this.messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
    }

    public static void main(String[] args) throws Throwable {
        final String localHost = args[0];
        final String masterHostAndPort = args[1];
        final String topics = args[2];
        final String group = args[3];
        final int consumeCount = Integer.parseInt(args[4]);

        final MessagePullConsumerExample messageConsumer = new MessagePullConsumerExample(
            localHost,
            masterHostAndPort,
            group);

        final List<String> topicList = Arrays.asList(topics.split(","));
        messageConsumer.subscribe(topicList);
        long startTime = System.currentTimeMillis();

        Thread[] fetchRunners = new Thread[3];
        for (int i = 0; i < fetchRunners.length; i++) {
            fetchRunners[i] = new Thread(new FetchRequestRunner(messageConsumer, consumeCount));
            fetchRunners[i].setName("_fetch_runner_" + i);
        }

        // wait for client to join the exact consumer queue that consumer group allocated
        while (!messageConsumer.isCurConsumeReady(1000)) {
            ThreadUtils.sleep(1000);
        }

        logger.info("Wait and get partitions use time " + (System.currentTimeMillis() - startTime));

        for (Thread thread : fetchRunners) {
            thread.start();
        }

        Thread statisticThread = new Thread(msgRecvStats, "Sent Statistic Thread");
        statisticThread.start();
    }

    public void subscribe(List<String> topicList) throws TubeClientException {
        for (String topic : topicList) {
            messagePullConsumer.subscribe(topic, null);
        }

        messagePullConsumer.completeSubscribe();
    }

    public boolean isCurConsumeReady(long waitTime) {
        return messagePullConsumer.isPartitionsReady(waitTime);
    }

    public ConsumerResult getMessage() throws TubeClientException {
        return messagePullConsumer.getMessage();
    }

    public ConsumerResult confirmConsume(final String confirmContext, boolean isConsumed) throws TubeClientException {
        return messagePullConsumer.confirmConsume(confirmContext, isConsumed);
    }

    public Map<String, ConsumeOffsetInfo> getCurrPartitionOffsetMap() throws TubeClientException {
        return messagePullConsumer.getCurConsumedPartitions();
    }

    private static class FetchRequestRunner implements Runnable {

        final MessagePullConsumerExample messageConsumer;
        final int consumeCount;

        FetchRequestRunner(final MessagePullConsumerExample messageConsumer, int count) {
            this.messageConsumer = messageConsumer;
            this.consumeCount = count;
        }

        @Override
        public void run() {
            try {
                int getCount = consumeCount;
                do {
                    ConsumerResult result = messageConsumer.getMessage();
                    if (result.isSuccess()) {
                        List<Message> messageList = result.getMessageList();
                        if (messageList != null && !messageList.isEmpty()) {
                            msgRecvStats.addMsgCount(result.getTopicName(), messageList.size());
                        }
                        messageConsumer.confirmConsume(result.getConfirmContext(), true);
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
                    if (consumeCount > 0) {
                        if (--getCount <= 0) {
                            break;
                        }
                    }
                } while (true);
                msgRecvStats.stopStats();
            } catch (TubeClientException e) {
                logger.error("Create consumer failed!", e);
            }
        }
    }
}


