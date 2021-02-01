/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.events;

import com.fasterxml.jackson.databind.ObjectReader;
import com.powsybl.afs.server.AppDataWrapper;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.events.AppStorageListener;
import com.powsybl.afs.storage.events.NodeEventContainer;
import com.powsybl.commons.json.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Web socket handler for node events :
 * <ul>
 *     <li>On connection, registers a listener to forward events to the client</li>
 *     <li>On receiving events from the client, forwards it to underlying event bus</li>
 * </ul>
 */
public class NodeEventHandler extends TextWebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeEventHandler.class);

    private static final ObjectReader NODE_EVENT_READER = JsonUtil.createObjectMapper()
            .readerFor(NodeEventContainer.class);

    private final AppDataWrapper appDataWrapper;
    private final WebSocketContext webSocketContext;

    public NodeEventHandler(AppDataWrapper appDataWrapper, WebSocketContext webSocketContext) {
        this.appDataWrapper = appDataWrapper;
        this.webSocketContext = webSocketContext;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String fileSystemName = SessionAttributes.of(session).getFilesystem();
        LOGGER.debug("WebSocket session '{}' opened for file system '{}'", session.getId(), fileSystemName);

        registerListener(session);
    }

    private void registerListener(WebSocketSession session) {
        AppStorageListener eventForwarder = new NodeEventForwarder(session);
        appDataWrapper.getAppData().getEventsBus().addListener(eventForwarder);
        SessionAttributes.of(session).setListener(eventForwarder);
        webSocketContext.addSession(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        LOGGER.trace("Node event websocket session '' of type '{}' id: ",
                session.getId());

        forwardEventsToBus(message);
    }

    private void forwardEventsToBus(TextMessage message) {
        try {
            NodeEventContainer container = NODE_EVENT_READER.readValue(message.getPayload());
            AppStorage storage = appDataWrapper.getStorage(container.getFileSystemName());
            storage.getEventsBus().pushEvent(container.getNodeEvent(), container.getTopic());
            storage.getEventsBus().flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        removeSession(session);
    }

    private void removeSession(WebSocketSession session) {
        AppStorageListener listener = SessionAttributes.of(session).getListener();
        appDataWrapper.getAppData().getEventsBus().removeListener(listener);
        webSocketContext.removeSession(session);
    }
}
