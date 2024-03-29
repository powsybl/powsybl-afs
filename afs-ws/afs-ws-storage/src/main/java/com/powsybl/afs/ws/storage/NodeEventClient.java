/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage;

import com.powsybl.afs.storage.events.AppStorageListener;
import com.powsybl.afs.storage.events.NodeEvent;
import com.powsybl.afs.storage.events.NodeEventContainer;
import com.powsybl.afs.storage.events.NodeEventList;
import com.powsybl.afs.ws.storage.websocket.WebsocketConnectionManager;
import com.powsybl.commons.util.WeakListenerList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.websocket.*;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@ClientEndpoint(decoders = {NodeEventListDecoder.class}, encoders = {NodeEventContainerEncoder.class})
public class NodeEventClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeEventClient.class);

    private final WebsocketConnectionManager connectionManager;
    private final String fileSystemName;
    private final WeakListenerList<AppStorageListener> listeners;

    private Session session = null;

    public NodeEventClient(WebsocketConnectionManager connectionManager, String fileSystemName, WeakListenerList<AppStorageListener> listeners) {
        this.connectionManager = Objects.requireNonNull(connectionManager);
        this.fileSystemName = Objects.requireNonNull(fileSystemName);
        this.listeners = Objects.requireNonNull(listeners);
    }

    public void connect() {
        connectionManager.connect(this);
    }

    public void pushEvent(NodeEvent event, String fileSystemName, String topic) {
        if (session.isOpen()) {
            RemoteEndpoint.Async remote = session.getAsyncRemote();
            remote.setSendTimeout(1000);
            remote.sendObject(new NodeEventContainer(event, fileSystemName, topic), result -> {
                if (!result.isOK()) {
                    LOGGER.error(result.getException().toString(), result.getException());
                }
            });
        }
    }

    @OnOpen
    public void onOpen(Session session) {
        LOGGER.trace("Node event websocket session '{}' opened for file system '{}'", session.getId(), fileSystemName);
        this.session = session;
    }

    @OnMessage
    public void onMessage(Session session, NodeEventList nodeEventList) {
        LOGGER.trace("Node event websocket session '{}' of file system '{}' received an event list: {}",
                session.getId(), fileSystemName, nodeEventList);
        listeners.log();
        listeners.notify(l -> {
            if (l.topics().isEmpty() || l.topics().contains(nodeEventList.getTopic())) {
                l.onEvents(nodeEventList);
            }
        });
    }

    @OnError
    public void onError(Throwable t) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error(t.toString(), t);
        }
    }

    @OnClose
    public void onClose(Session session) {
        LOGGER.trace("Node event websocket session '{}' closed for file system '{}'", session.getId(), fileSystemName);
        connectionManager.onClose(session, this);
    }

    @Override
    public void close() {

        try {
            //First close the connection manager, to ensure it does not try to reconnect
            connectionManager.close();
            if (session != null) {
                session.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
