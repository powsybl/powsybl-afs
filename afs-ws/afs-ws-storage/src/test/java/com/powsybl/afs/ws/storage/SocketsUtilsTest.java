/*
 * Copyright (c) 2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ws.storage;

import com.powsybl.commons.exceptions.UncheckedUriSyntaxException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class SocketsUtilsTest {

    @Test
    void test() {
        int port = 40903;

        // HTTP
        try {
            String sheme = "http";
            URI uri = new URI(sheme + "://localhost:" + port);
            URI wsUri = SocketsUtils.getWebSocketUri(uri);

            assertEquals("ws", wsUri.getScheme());
            assertEquals("localhost", wsUri.getHost());
            assertEquals(port, wsUri.getPort());
        } catch (URISyntaxException e) {
            throw new UncheckedUriSyntaxException(e);
        }

        // HTTPS
        try {
            String sheme = "https";
            URI uri = new URI(sheme + "://localhost:" + port);
            URI wsUri = SocketsUtils.getWebSocketUri(uri);

            assertEquals("wss", wsUri.getScheme());
            assertEquals("localhost", wsUri.getHost());
            assertEquals(port, wsUri.getPort());
        } catch (URISyntaxException e) {
            throw new UncheckedUriSyntaxException(e);
        }
    }
}
