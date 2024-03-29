/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server;

import jakarta.enterprise.inject.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import jakarta.websocket.CloseReason;
import jakarta.websocket.CloseReason.CloseCodes;
import jakarta.websocket.Session;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@Singleton
@Model
public class WebSocketContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketContext.class);

    private final Set<Session> sessions = new HashSet<>();

    public synchronized void addSession(Session session) {
        sessions.add(session);
    }

    public synchronized void removeSession(Session session) {
        sessions.remove(session);
    }

    @PreDestroy
    public synchronized void closeAndRemoveAllSessions() {
        for (Session session : sessions) {
            try {
                session.close(new CloseReason(CloseCodes.UNEXPECTED_CONDITION, ""));
            } catch (Exception e) {
                LOGGER.error(e.toString(), e);
            }
        }
        sessions.clear();
    }

}
