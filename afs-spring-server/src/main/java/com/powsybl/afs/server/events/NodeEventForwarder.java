/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.events;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.powsybl.afs.storage.events.AppStorageListener;
import com.powsybl.afs.storage.events.NodeEventList;
import com.powsybl.commons.json.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

/**
 * A listener which forwards events to a websocket client.
 */
public class NodeEventForwarder implements AppStorageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeEventForwarder.class);

    private static final ObjectWriter NODE_EVENT_WRITER = JsonUtil.createObjectMapper().writerFor(NodeEventList.class);

    private final WebSocketSession session;

    NodeEventForwarder(WebSocketSession session) {
        this.session = WebSocketUtils.concurrent(session);
    }

    @Override
    public void onEvents(NodeEventList eventList) {
        if (session.isOpen()) {
            try {
                String eventListEncode = NODE_EVENT_WRITER.writeValueAsString(eventList);
                session.sendMessage(new TextMessage(eventListEncode));
            } catch (Exception e) {
                LOGGER.error("Failed to send events {} to {}:", eventList, session.getRemoteAddress(), e);
            }
        } else {
            LOGGER.error("Could not send events {} to client {}: session is closed.",
                    eventList, session.getRemoteAddress());
        }
    }
}
