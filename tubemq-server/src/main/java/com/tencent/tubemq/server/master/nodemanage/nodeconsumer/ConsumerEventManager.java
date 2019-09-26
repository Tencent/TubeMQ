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

package com.tencent.tubemq.server.master.nodemanage.nodeconsumer;

import com.tencent.tubemq.corebase.balance.ConsumerEvent;
import com.tencent.tubemq.corebase.cluster.ConsumerInfo;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerEventManager {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerEventManager.class);

    private final ConcurrentHashMap<String/* consumerId */, LinkedList<ConsumerEvent>> disconnectEventMap =
            new ConcurrentHashMap<String, LinkedList<ConsumerEvent>>();
    private final ConcurrentHashMap<String/* consumerId */, LinkedList<ConsumerEvent>> connectEventMap =
            new ConcurrentHashMap<String, LinkedList<ConsumerEvent>>();
    private final ConcurrentHashMap<String/* group */, AtomicInteger> groupUnfinishedCountMap =
            new ConcurrentHashMap<String, AtomicInteger>();

    private final ConsumerInfoHolder consumerHolder;

    public ConsumerEventManager(ConsumerInfoHolder consumerHolder) {
        this.consumerHolder = consumerHolder;
    }

    public boolean addDisconnectEvent(String consumerId,
                                      ConsumerEvent event) {
        LinkedList<ConsumerEvent> eventList =
                disconnectEventMap.get(consumerId);
        if (eventList == null) {
            eventList = new LinkedList<ConsumerEvent>();
            LinkedList<ConsumerEvent> tmptList =
                    disconnectEventMap.putIfAbsent(consumerId, eventList);
            if (tmptList != null) {
                eventList = tmptList;
            }
        }
        synchronized (eventList) {
            return eventList.add(event);
        }
    }

    public boolean addConnectEvent(String consumerId,
                                   ConsumerEvent event) {
        LinkedList<ConsumerEvent> eventList =
                connectEventMap.get(consumerId);
        if (eventList == null) {
            eventList = new LinkedList<ConsumerEvent>();
            LinkedList<ConsumerEvent> tmptList =
                    connectEventMap.putIfAbsent(consumerId, eventList);
            if (tmptList != null) {
                eventList = tmptList;
            }
        }
        synchronized (eventList) {
            return eventList.add(event);
        }
    }

    /**
     * Peek a consumer event from event map,
     * disconnect event have priority over connect event
     *
     * @param consumerId the consumer id
     * @return the head of event list, or null if event list is empty
     */
    public ConsumerEvent peek(String consumerId) {
        String group =
                consumerHolder.getGroup(consumerId);
        if (group != null) {
            ConcurrentHashMap<String, LinkedList<ConsumerEvent>> currentEventMap =
                    hasDisconnectEvent(group)
                            ? disconnectEventMap : connectEventMap;
            LinkedList<ConsumerEvent> eventList =
                    currentEventMap.get(consumerId);
            if (eventList != null) {
                synchronized (eventList) {
                    return eventList.peek();
                }
            }
        } else {
            logger.warn(new StringBuilder(512)
                    .append("No group by consumer ")
                    .append(consumerId).toString());
        }
        return null;
    }

    /**
     * Removes and returns the first consumer event from event map
     * disconnect event have priority over connect event
     *
     * @param consumerId
     * @return the first consumer removed from the event map
     */
    public ConsumerEvent removeFirst(String consumerId) {
        ConsumerEvent event = null;
        String group = consumerHolder.getGroup(consumerId);
        ConcurrentHashMap<String, LinkedList<ConsumerEvent>> currentEventMap =
                hasDisconnectEvent(group) ? disconnectEventMap : connectEventMap;
        LinkedList<ConsumerEvent> eventList = currentEventMap.get(consumerId);
        if (eventList != null) {
            synchronized (eventList) {
                if (CollectionUtils.isNotEmpty(eventList)) {
                    event = eventList.removeFirst();
                    if (eventList.isEmpty()) {
                        currentEventMap.remove(consumerId);
                    }
                }
            }
        }
        if (event == null) {
            logger.info("[Event Removed] event is null");
        } else {
            StringBuilder sBuilder = new StringBuilder(512);
            sBuilder.append("[Event Removed] ");
            logger.info(event.toStrBuilder(sBuilder).toString());
        }
        return event;
    }

    public int getUnfinishedCount(String groupName) {
        if (groupName == null) {
            return 0;
        }
        AtomicInteger unfinishedCount =
                groupUnfinishedCountMap.get(groupName);
        if (unfinishedCount == null) {
            return 0;
        }
        return unfinishedCount.get();
    }

    public void updateUnfinishedCountMap(Set<String> groupHasUnfinishedEvent) {
        if (groupHasUnfinishedEvent.isEmpty()) {
            groupUnfinishedCountMap.clear();
        } else {
            for (String oldGroup : groupUnfinishedCountMap.keySet()) {
                if (oldGroup != null) {
                    if (!groupHasUnfinishedEvent.contains(oldGroup)) {
                        groupUnfinishedCountMap.remove(oldGroup);
                    }
                }
            }
            for (String newGroup : groupHasUnfinishedEvent) {
                if (newGroup != null) {
                    AtomicInteger unfinishedCount =
                            groupUnfinishedCountMap.get(newGroup);
                    if (unfinishedCount == null) {
                        AtomicInteger newCount = new AtomicInteger(0);
                        unfinishedCount =
                                groupUnfinishedCountMap.putIfAbsent(newGroup, newCount);
                        if (unfinishedCount == null) {
                            unfinishedCount = newCount;
                        }
                    }
                    unfinishedCount.incrementAndGet();
                }
            }
        }
    }

    public void removeAll(String consumerId) {
        disconnectEventMap.remove(consumerId);
        connectEventMap.remove(consumerId);
    }

    /**
     * Check if event map have event including disconnect event and connect event
     *
     * @return true if event map is not empty, otherwise false
     */
    public boolean hasEvent() {
        for (Map.Entry<String, LinkedList<ConsumerEvent>> entry
                : disconnectEventMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                return true;
            }
        }
        for (Map.Entry<String, LinkedList<ConsumerEvent>> entry
                : connectEventMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if disconnect event map have event
     *
     * @param group
     * @return true if disconnect event map not empty otherwise false
     */
    public boolean hasDisconnectEvent(String group) {
        List<ConsumerInfo> consumerList =
                consumerHolder.getConsumerList(group);
        if (CollectionUtils.isNotEmpty(consumerList)) {
            for (ConsumerInfo consumer : consumerList) {
                List<ConsumerEvent> eventList =
                        disconnectEventMap.get(consumer.getConsumerId());
                if (eventList != null) {
                    synchronized (eventList) {
                        if (CollectionUtils.isNotEmpty(eventList)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Get all consumer id which have unfinished event
     *
     * @return consumer id set
     */
    public Set<String> getUnProcessedIdSet() {
        Set<String> consumerIdSet = new HashSet<String>();
        for (Map.Entry<String, LinkedList<ConsumerEvent>> entry
                : disconnectEventMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                consumerIdSet.add(entry.getKey());
            }
        }
        for (Map.Entry<String, LinkedList<ConsumerEvent>> entry
                : connectEventMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                consumerIdSet.add(entry.getKey());
            }
        }
        return consumerIdSet;
    }

    public void clear() {
        disconnectEventMap.clear();
        connectEventMap.clear();
    }

    @Override
    public String toString() {
        return "ConsumerEventManager [disconnectEventMap=" + disconnectEventMap + ", connectEventMap="
                + connectEventMap + "]";
    }
}
