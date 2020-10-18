/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.Session;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A connection manager which will attempt to reconnect at fixed time intervals.
 *
 * @author Sylvain Leclerc <sylvain.leclerc@rte-france.com>
 */
public class AutoReconnectionConnectionManager extends StandardConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AutoReconnectionConnectionManager.class);

    private final ScheduledExecutorService scheduler;
    private final long delay;
    private final TimeUnit timeUnit;

    private boolean closed = false;

    public AutoReconnectionConnectionManager(URI uri, long delay, TimeUnit timeUnit) {
        super(uri);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.delay = delay;
        this.timeUnit = timeUnit;
    }

    public AutoReconnectionConnectionManager(URI uri, long delayInSeconds) {
        this(uri, delayInSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void onClose(Session session, Object clientEndoint) {
        if (!closed) {
            LOGGER.warn("Connection to {} has been closed, will try to reconnect in {}", getUri(), getDelayAsString());
            scheduleReconnection(clientEndoint);
        }
    }

    private void scheduleReconnection(Object clientEndoint) {
        scheduler.schedule(() -> attemptReconnection(clientEndoint), delay, timeUnit);
    }

    private void attemptReconnection(Object clientEndoint) {
        if (closed) {
            return;
        }
        try {
            connect(clientEndoint);
        } catch (Exception e) {
            LOGGER.error("Failed to reconnect to {}, will retry in {}", getUri(), getDelayAsString(), timeUnit, e);
            scheduleReconnection(clientEndoint);
        }
    }

    private String getDelayAsString() {
        return delay + " " + timeUnit.toString().toLowerCase();
    }

    @Override
    public void close() {
        closed = true;
    }
}
