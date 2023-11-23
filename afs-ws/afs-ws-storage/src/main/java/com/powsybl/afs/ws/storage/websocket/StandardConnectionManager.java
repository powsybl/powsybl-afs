/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage.websocket;

import com.powsybl.afs.ws.client.utils.UncheckedDeploymentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Objects;

/**
 * A simple connection manager which does not try to reconnect on close.
 *
 * @author Sylvain Leclerc {@literal <sylvain.leclerc@rte-france.com>}
 */
public class StandardConnectionManager implements WebsocketConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardConnectionManager.class);

    private final WebSocketContainer webSocketContainer;
    private final URI uri;

    public StandardConnectionManager(URI uri) {
        this.webSocketContainer = ContainerProvider.getWebSocketContainer();
        this.uri = Objects.requireNonNull(uri);
    }

    protected URI getUri() {
        return uri;
    }

    @Override
    public Session connect(Object clientEndoint) {
        try {
            LOGGER.debug("Connecting to websocket server at {}.", uri);
            return webSocketContainer.connectToServer(clientEndoint, uri);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DeploymentException e) {
            throw new UncheckedDeploymentException(e);
        }
    }

    @Override
    public void onClose(Session session, Object clientEndoint) {
        //nothing
    }

    @Override
    public void close() {
        //nothing
    }
}
