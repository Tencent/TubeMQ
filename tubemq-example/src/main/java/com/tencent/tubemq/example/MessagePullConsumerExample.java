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


public final class MessagePullConsumerExample {

    private static final Logger logger =
            LoggerFactory.getLogger(MessagePullConsumerExample.class);
    private static final MsgRecvStats msgRecvStats = new MsgRecvStats();
    private final String masterHostAndPort;
    private final String group;
    private final String localHost;
    private PullMessageConsumer messagePullConsumer;
    private MessageSessionFactory messageSessionFactory;

    public MessagePullConsumerExample(String localHost, String masterHostAndPort, String group) throws Exception {
        this.masterHostAndPort = masterHostAndPort;
        this.group = group;
        this.localHost = localHost;
        ConsumerConfig consumerConfig =
                new ConsumerConfig(this.localHost, this.masterHostAndPort, this.group);
        consumerConfig.setConsumeModel(0);
        this.messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        this.messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
    }

    public static void main(String[] args) throws Throwable {
        final String localHost = args[0];
        final String masterHostAndPort = args[1];
        final String topics = args[2];
        final List<String> topicList = Arrays.asList(topics.split(","));
        final Thread statisticThread;
        final String group = args[3];
        final int consumeCount = Integer.parseInt(args[4]);
        Thread[] fetchRunners;
        fetchRunners = new Thread[3];


        final MessagePullConsumerExample messageConsumer =
                new MessagePullConsumerExample(localHost, masterHostAndPort, group);
        messageConsumer.subscribe(topicList);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < fetchRunners.length; i++) {
            fetchRunners[i] = new Thread(new FetchRequestRunner(messageConsumer, consumeCount));
            fetchRunners[i].setName("_fetch_runner_" + i);
        }


        //等待客户端加入消费组分配实际消费的队列
        while (!messageConsumer.isCurConsumeReady(1000)) {
            ThreadUtils.sleep(1000);
        }
        logger.info("Wait and get partitions use time " + (System.currentTimeMillis() - startTime));

        for (final Thread thread : fetchRunners) {
            thread.start();
        }

        statisticThread = new Thread(msgRecvStats, "Sent Statistic Thread");
        statisticThread.start();
    }

    public void subscribe(List<String> topicList) throws TubeClientException {
        for (String topic : topicList) {
            this.messagePullConsumer.subscribe(topic, null);
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

    static class FetchRequestRunner implements Runnable {

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
                        long currTime = System.currentTimeMillis();
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
                                logger.info("Receive messages errorCode is "
                                        + result.getErrCode() + ", Error message is " + result.getErrMsg());
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


