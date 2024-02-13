/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage.websocket;

import jakarta.websocket.Session;

/**
 * Responsible for managing the connection to a websocket server endpoint.
 *
 * @author Sylvain Leclerc {@literal <sylvain.leclerc@rte-france.com>}
 */
public interface WebsocketConnectionManager extends AutoCloseable {

    /**
     * Connects the client to the websocket server.
     */
    Session connect(Object clientEndoint);

    /**
     * To be called by client on session close. The implementation
     * may trigger any reconnection logic here.
     */
    void onClose(Session session, Object clientEndoint);

    /**
     * To be called by client when it does not need to connect anymore.
     */
    @Override
    void close();

}
