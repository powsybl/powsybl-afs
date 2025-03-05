/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ws.client.utils;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class ClientUtilsTest {

    @Test
    void testCreateClient() {
        Client client = ClientUtils.createClient();
        assertNotNull(client);
    }

    @Test
    void testCheckOkWithOkResponse() {
        try (Response response = mock(Response.class)) {
            when(response.getStatus()).thenReturn(Response.Status.OK.getStatusCode());

            assertDoesNotThrow(() -> ClientUtils.checkOk(response));
        }
    }

    @Test
    void testCheckOkWithServerError() {
        try (Response response = mock(Response.class)) {
            when(response.getStatus()).thenReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
            when(response.readEntity(String.class)).thenReturn("{\"javaException\":\"java.lang.IllegalArgumentException\", \"message\":\"Error occurred\"}");

            assertThrows(IllegalArgumentException.class, () -> ClientUtils.checkOk(response));
        }
    }

    @Test
    void testReadEntityIfOkWithOkResponse() {
        try (Response response = mock(Response.class)) {
            when(response.getStatus()).thenReturn(Response.Status.OK.getStatusCode());
            when(response.readEntity(String.class)).thenReturn("Test entity");

            String result = ClientUtils.readEntityIfOk(response, String.class);
            assertEquals("Test entity", result);
        }
    }

    @Test
    void testReadOptionalEntityIfOkWithNotFound() {
        try (Response response = mock(Response.class)) {
            when(response.getStatus()).thenReturn(Response.Status.NOT_FOUND.getStatusCode());

            Optional<String> result = ClientUtils.readOptionalEntityIfOk(response, String.class);
            assertTrue(result.isEmpty());
        }
    }
}
