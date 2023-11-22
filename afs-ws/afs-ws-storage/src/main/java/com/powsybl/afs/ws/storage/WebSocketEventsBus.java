/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage;

import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.EventsBus;
import com.powsybl.afs.storage.events.AppStorageListener;
import com.powsybl.afs.storage.events.NodeEvent;
import com.powsybl.afs.ws.storage.websocket.WebsocketConnectionPolicy;
import com.powsybl.afs.ws.utils.AfsRestApi;
import com.powsybl.commons.util.WeakListenerList;

import java.net.URI;
import java.util.Objects;

/**
 * @author Chamseddine Benhamed {@literal <chamseddine.benhamed at rte-france.com>}
 */
public class WebSocketEventsBus implements EventsBus {

    private final WeakListenerList<AppStorageListener> listeners = new WeakListenerList<>();

    private final NodeEventClient nodeEventClient;

    private final AppStorage storage;

    public WebSocketEventsBus(AppStorage storage, URI restUri, WebsocketConnectionPolicy connectionPolicy) {
        URI wsUri = SocketsUtils.getWebSocketUri(restUri);
        this.storage = Objects.requireNonNull(storage);
        URI endPointUri = URI.create(wsUri + "/messages/" + AfsRestApi.RESOURCE_ROOT + "/" +
                AfsRestApi.VERSION + "/node_events/" + storage.getFileSystemName());

        nodeEventClient = new NodeEventClient(connectionPolicy.newConnectionManager(endPointUri), storage.getFileSystemName(), listeners);
        nodeEventClient.connect();
    }

    @Override
    public void pushEvent(NodeEvent event, String topic) {
        nodeEventClient.pushEvent(event, storage.getFileSystemName(), topic);
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

    @Override
    public void flush() {
        // Noop
    }

    @Override
    public void close() {
        nodeEventClient.close();
    }
}
