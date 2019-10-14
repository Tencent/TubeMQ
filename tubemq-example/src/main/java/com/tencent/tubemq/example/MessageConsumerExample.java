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
import com.tencent.tubemq.client.consumer.MessageListener;
import com.tencent.tubemq.client.consumer.PushMessageConsumer;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.factory.MessageSessionFactory;
import com.tencent.tubemq.client.factory.TubeSingleSessionFactory;
import com.tencent.tubemq.corebase.Message;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to consume message sequentially.
 *
 * <p>Consumer supports subscribe multiple topics in one consume group. Message from subscription
 * sent back to business logic via callback {@link MessageListener}. It is highly recommended NOT
 * to perform any blocking operation inside the callback.
 *
 * <p>As for consumption control of {@link PushMessageConsumer}, business logic is able to monitor
 * current state and adjust consumption by
 *
 * <p><ul>
 *     <li>call {@link PushMessageConsumer#pauseConsume()} to pause consumption when high water mark exceeded.</li>
 *     <li>call {@link PushMessageConsumer#resumeConsume()} to resume consumption</li>
 * </ul>
 */
public final class MessageConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerExample.class);
    private static final MsgRecvStats msgRecvStats = new MsgRecvStats();

    private final PushMessageConsumer messageConsumer;
    private final MessageSessionFactory messageSessionFactory;

    public MessageConsumerExample(
        String localHost,
        String masterHostAndPort,
        String group,
        int fetchCount
    ) throws Exception {
        ConsumerConfig consumerConfig = new ConsumerConfig(localHost, masterHostAndPort, group);
        consumerConfig.setConsumeModel(0);
        if (fetchCount > 0) {
            consumerConfig.setPushFetchThreadCnt(fetchCount);
        }
        this.messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        this.messageConsumer = messageSessionFactory.createPushConsumer(consumerConfig);
    }

    public static void main(String[] args) {
        final String localHost = args[0];
        final String masterHostAndPort = args[1];
        final String topics = args[2];
        final String group = args[3];
        final int consumerCount = Integer.parseInt(args[4]);
        int fetchCount = -1;
        if (args.length > 5) {
            fetchCount = Integer.parseInt(args[5]);
        }
        final Map<String, TreeSet<String>> topicTidsMap = new HashMap<>();

        String[] topicTidsList = topics.split(",");
        for (String topicTids : topicTidsList) {
            String[] topicTidStr = topicTids.split(":");
            TreeSet<String> tids = null;
            if (topicTidStr.length > 1) {
                String tidsStr = topicTidStr[1];
                String[] tidsSet = tidsStr.split(";");
                if (tidsSet.length > 0) {
                    tids = new TreeSet<>(Arrays.asList(tidsSet));
                }
            }
            topicTidsMap.put(topicTidStr[0], tids);
        }
        final int startFetchCnt = fetchCount;
        final ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < consumerCount; i++) {
                        MessageConsumerExample messageConsumer = new MessageConsumerExample(
                                localHost,
                                masterHostAndPort,
                                group,
                                startFetchCnt);
                        messageConsumer.subscribe(topicTidsMap);
                    }
                } catch (Exception e) {
                    logger.error("Create consumer failed!", e);
                }
            }
        });

        final Thread statisticThread = new Thread(msgRecvStats, "Sent Statistic Thread");
        statisticThread.start();

        executorService.shutdown();
        try {
            executorService.awaitTermination(60 * 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Thread Pool shutdown has been interrupted!");
        }
        msgRecvStats.stopStats();
    }

    public void subscribe(Map<String, TreeSet<String>> topicTidsMap) throws TubeClientException {
        for (Map.Entry<String, TreeSet<String>> entry : topicTidsMap.entrySet()) {
            MessageListener messageListener = new DefaultMessageListener(entry.getKey());
            messageConsumer.subscribe(entry.getKey(), entry.getValue(), messageListener);
        }
        messageConsumer.completeSubscribe();
    }

    public static class DefaultMessageListener implements MessageListener {

        private String topic;

        public DefaultMessageListener(String topic) {
            this.topic = topic;
        }

        public void receiveMessages(List<Message> messages) {
            if (messages != null && !messages.isEmpty()) {
                msgRecvStats.addMsgCount(this.topic, messages.size());
            }
        }

        public Executor getExecutor() {
            return null;
        }

        public void stop() {
        }
    }
}
