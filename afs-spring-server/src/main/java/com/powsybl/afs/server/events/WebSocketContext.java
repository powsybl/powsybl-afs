/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Component
public class WebSocketContext implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketContext.class);

    private final Set<WebSocketSession> sessions = new HashSet<>();

    public synchronized void addSession(WebSocketSession session) {
        sessions.add(session);
    }

    public synchronized void removeSession(WebSocketSession session) {
        sessions.remove(session);
    }

    @Override
    public synchronized void close() {
        for (WebSocketSession session : sessions) {
            try {
                session.close(CloseStatus.SERVER_ERROR);
            } catch (Exception e) {
                LOGGER.error("An error occurred while closing web socket sessions", e);
            }
        }
        sessions.clear();
    }

}
