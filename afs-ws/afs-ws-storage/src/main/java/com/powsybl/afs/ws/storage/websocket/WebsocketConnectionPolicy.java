/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage.websocket;

import com.powsybl.afs.ws.client.utils.RemoteServiceConfig;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Defines how websockets clients should connect to server endpoints,
 * and in particular how the loss of connection should be handled.
 *
 * <p>The actual connection logic is implemented in {@link WebsocketConnectionManager} instances.
 *
 * @author Sylvain Leclerc {@literal <sylvain.leclerc@rte-france.com>}
 */
public interface WebsocketConnectionPolicy {

    /**
     * Creates a new connection manager for the specified URI.
     */
    WebsocketConnectionManager newConnectionManager(URI uri);

    /**
     * The standard connection policy :
     * no reconnection will be attempted on connection loss.
     */
    static WebsocketConnectionPolicy standard() {
        return StandardConnectionManager::new;
    }

    /**
     * A reconnection policy which will attempt to reconnect at fixed time interval,
     * when a websocket session is closed.
     * The connection manager will stop retrying to connect once it is closed.
     */
    static WebsocketConnectionPolicy withAutoReconnection(long delay, TimeUnit timeUnit) {
        return uri -> new AutoReconnectionConnectionManager(uri, delay, timeUnit);
    }

    /**
     * Creates a policy according to the remote service config.
     */
    static WebsocketConnectionPolicy forConfig(RemoteServiceConfig config) {
        if (config.isAutoReconnectionEnabled()) {
            return withAutoReconnection(config.getReconnectionDelay(), TimeUnit.SECONDS);
        }
        return standard();
    }
}
