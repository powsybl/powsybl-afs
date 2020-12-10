/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.events;

import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

/**
 * Utilities for web sockets.
 *
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public final class WebSocketUtils {

    private WebSocketUtils() {
    }

    /**
     * Wraps the provided session into a thread safe session.
     */
    public static WebSocketSession concurrent(WebSocketSession session) {
        return new ConcurrentWebSocketSessionDecorator(session,
                WebSocketConstants.SEND_TIMEOUT_MILLIS,
                WebSocketConstants.SEND_BUFFER_SIZE,
                ConcurrentWebSocketSessionDecorator.OverflowStrategy.DROP);
    }
}
