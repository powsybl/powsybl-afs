/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ws.client.utils;

import com.powsybl.commons.net.UserProfile;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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

    @Test
    @Disabled("Fix me")
    void testAuthenticate() {
        URI baseUri = URI.create("http://localhost");
        String login = "user";
        String password = "password";

        try (MockedStatic<ClientUtils> mockedClientUtils = mockStatic(ClientUtils.class)) {
            Client client = mock(Client.class);
            WebTarget webTarget = mock(WebTarget.class);
            Invocation.Builder builder = mock(Invocation.Builder.class);
            Response response = mock(Response.class);

            mockedClientUtils.when(ClientUtils::createClient).thenReturn(client);

            when(client.target(baseUri)).thenReturn(webTarget);
            when(webTarget.path("rest")).thenReturn(webTarget);
            when(webTarget.path("users")).thenReturn(webTarget);
            when(webTarget.path("login")).thenReturn(webTarget);
            when(webTarget.request()).thenReturn(builder);
            when(builder.post(any(Entity.class))).thenReturn(response);
            when(response.getStatus()).thenReturn(Response.Status.OK.getStatusCode());
            when(response.readEntity(UserProfile.class)).thenReturn(new UserProfile("firstName", "lastName"));
            when(response.getHeaderString(Mockito.anyString())).thenReturn("auth-token");

            UserSession session = ClientUtils.authenticate(baseUri, login, password);
            assertNotNull(session);
            assertEquals("auth-token", session.getToken());
        }
    }
}
