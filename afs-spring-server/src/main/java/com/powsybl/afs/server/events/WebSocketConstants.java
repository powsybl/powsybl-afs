/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.events;

/**
 * Constants for web sockets.
 *
 * @author Sylvain Leclerc {@literal <sylvain.leclerc@rte-france.com>}
 */
public final class WebSocketConstants {

    private WebSocketConstants() {
    }

    /**
     * Sends will timeout after 10s
     */
    public static final int SEND_TIMEOUT_MILLIS = 10 * 1000;

    /**
     * 1M buffer size
     */
    public static final int SEND_BUFFER_SIZE = 1024 * 1024;
}
