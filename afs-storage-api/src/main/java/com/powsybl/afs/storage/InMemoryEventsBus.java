/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import com.powsybl.afs.storage.events.AppStorageListener;
import com.powsybl.afs.storage.events.NodeEvent;
import com.powsybl.afs.storage.events.NodeEventList;
import com.powsybl.commons.util.WeakListenerList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Chamseddine Benhamed {@literal <chamseddine.benhamed at rte-france.com>}
 */
public class InMemoryEventsBus implements EventsBus {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryEventsBus.class);

    private final List<NodeEventList> topics = new ArrayList<>();

    private final WeakListenerList<AppStorageListener> listeners = new WeakListenerList<>();

    private final Lock lock = new ReentrantLock();

    @Override
    public void pushEvent(NodeEvent event, String topic) {
        lock.lock();
        NodeEventList lastEventTopic = topics.iterator().hasNext() ? topics.iterator().next() : null;
        if (lastEventTopic != null && Objects.equals(lastEventTopic.getTopic(), topic)) {
            lastEventTopic.addEvent(event);
        } else {
            topics.add(new NodeEventList(topic, event));
        }
        lock.unlock();
    }

    List<NodeEventList> getTopics() {
        return topics;
    }

    @Override
    public void flush() {
        List<NodeEventList> eventsToSend;
        lock.lock();
        try {
            // to prevent the same thread to reenter the lock and modify the list : a listener that produces other events
            eventsToSend = new ArrayList<>(topics);
            topics.clear();
        } finally {
            lock.unlock();
        }

        listeners.log();
        listeners.notify(l -> eventsToSend.forEach(nodeEventList -> {
            if (l.topics().isEmpty() || l.topics().contains(nodeEventList.getTopic())) {
                try {
                    l.onEvents(new NodeEventList(Collections.unmodifiableList(nodeEventList.getEvents()), nodeEventList.getTopic()));
                } catch (Exception e) {
                    LOGGER.error("Handler failed to consume events {}", nodeEventList, e);
                }
            }
        }));

    }

    @Override
    public void addListener(AppStorageListener l) {
        listeners.add(l);
    }

    @Override
    public void removeListener(AppStorageListener l) {
        listeners.remove(l);
    }

    @Override
    public void removeListeners() {
        listeners.removeAll();
    }
}
