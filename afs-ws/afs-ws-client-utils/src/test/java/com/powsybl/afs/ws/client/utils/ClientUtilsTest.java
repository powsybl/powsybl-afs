/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ws.client.utils;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.storage.AfsNodeNotFoundException;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.ws.utils.JsonProvider;
import com.powsybl.commons.net.UserProfile;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
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
    void testReadEntityIfOkWithNotFoundResponse() {
        String exceptionBody = """
            {
              "javaException" : "com.powsybl.afs.storage.AfsNodeNotFoundException",
              "message" : "Node faac9243-1314-421e-86cd-7bc3ced884bd not found"
            }""";
        try (Response response = Response
            .status(Response.Status.NOT_FOUND)
            .type(MediaType.APPLICATION_JSON)
            .entity(exceptionBody)
            .build()) {

            AfsNodeNotFoundException exception = assertThrows(AfsNodeNotFoundException.class, () -> ClientUtils.readEntityIfOk(response, String.class));
            assertEquals("Node faac9243-1314-421e-86cd-7bc3ced884bd not found", exception.getMessage());
        }
    }

    @Test
    void createSpecificExceptionNoExceptionMessageTest() {
        String exceptionBody = """
            {
              "javaException" : "com.powsybl.afs.storage.AfsNodeNotFoundException"
            }""";
        try (Response response = Response
            .status(Response.Status.NOT_FOUND)
            .type(MediaType.APPLICATION_JSON)
            .entity(exceptionBody)
            .build()) {

            AfsNodeNotFoundException exception = assertThrows(AfsNodeNotFoundException.class, () -> ClientUtils.readEntityIfOk(response, String.class));
            assertNull(exception.getMessage());
        }
    }

    @Test
    void createSpecificExceptionNoJavaExceptionTest() {
        String exceptionBody = """
            {
              "message" : "Node faac9243-1314-421e-86cd-7bc3ced884bd not found"
            }""";
        try (Response response = Response
            .status(Response.Status.NOT_FOUND)
            .type(MediaType.APPLICATION_JSON)
            .entity(exceptionBody)
            .build()) {

            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> ClientUtils.readEntityIfOk(response, String.class));
            assertEquals("No exception was found in response", exception.getMessage());
        }
    }

    @Test
    void createSpecificExceptionNoCorrespondingExceptionFoundTest() {
        // Unexpected type of exception for the Response status
        String exceptionBody = """
            {
              "javaException" : "com.powsybl.afs.storage.AfsStorageException",
              "message" : "Node faac9243-1314-421e-86cd-7bc3ced884bd not found"
            }""";
        try (Response response = Response
            .status(Response.Status.NOT_FOUND)
            .type(MediaType.APPLICATION_JSON)
            .entity(exceptionBody)
            .build()) {

            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> ClientUtils.readEntityIfOk(response, String.class));
            assertEquals("No corresponding exception class was found in: AfsNodeNotFoundException, AfsFileSystemNotFoundException", exception.getMessage());
        }
    }

    @Test
    void createSpecificExceptionUnknownExceptionTest() {
        // Unknown exception (here, it's the wrong package)
        String exceptionBody = """
            {
              "javaException" : "com.powsybl.afs.storage.AfsException",
              "message" : "Node faac9243-1314-421e-86cd-7bc3ced884bd not found"
            }""";
        try (Response response = Response
            .status(Response.Status.NOT_FOUND)
            .type(MediaType.APPLICATION_JSON)
            .entity(exceptionBody)
            .build()) {

            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> ClientUtils.readEntityIfOk(response, String.class));
            assertEquals("Reflexion exception: com.powsybl.afs.storage.AfsException", exception.getMessage());
        }
    }

    @Test
    void createExceptionAccordingToResponseBadRequestTest() {
        String exceptionBody = """
            {
              "javaException" : "com.powsybl.afs.AfsException",
              "message" : "Exception message"
            }""";
        try (Response response = Response
            .status(Response.Status.BAD_REQUEST)
            .type(MediaType.APPLICATION_JSON)
            .entity(exceptionBody)
            .build()) {

            AfsException exception = assertThrows(AfsException.class, () -> ClientUtils.readEntityIfOk(response, String.class));
            assertEquals("Exception message", exception.getMessage());
        }
    }

    @Test
    void createExceptionAccordingToResponseUnexpectedStatusTest() {
        String exceptionBody = """
            {
              "javaException" : "com.powsybl.afs.AfsException",
              "message" : "Exception message"
            }""";
        try (Response response = Response
            .status(Response.Status.UNAUTHORIZED)
            .type(MediaType.APPLICATION_JSON)
            .entity(exceptionBody)
            .build()) {

            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> ClientUtils.readEntityIfOk(response, String.class));
            assertEquals("Unexpected response status: 'Unauthorized'", exception.getMessage());
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
    void authenticateTest() {
        URI baseUri = URI.create("http://localhost");
        String login = "user";
        String password = "password";

        try (MockedStatic<ClientUtils> mockedClientUtils = mockStatic(ClientUtils.class, Mockito.CALLS_REAL_METHODS)) {
            // Mock the required elements
            Client client = mock(Client.class);
            WebTarget webTarget = mock(WebTarget.class);
            Invocation.Builder builder = mock(Invocation.Builder.class);
            Response response = mock(Response.class);

            // Mock configuration - createClient
            doReturn(client)  // Retourne le mock client après register()
                .when(client)
                .register(any(JsonProvider.class));
            mockedClientUtils.when(ClientUtils::createClient).thenReturn(client);

            // Mock configuration - client -> WebTarget -> Builder -> Response
            when(client.target(baseUri)).thenReturn(webTarget);
            when(webTarget.path(anyString())).thenReturn(webTarget);
            when(webTarget.request()).thenReturn(builder);
            when(builder.post(any(Entity.class))).thenReturn(response);

            // Mock configuration - simulate response
            when(response.getStatus()).thenReturn(Response.Status.OK.getStatusCode());
            when(response.readEntity(UserProfile.class)).thenReturn(new UserProfile("firstName", "lastName"));
            when(response.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("auth-token");

            // Call the tested method
            UserSession session = ClientUtils.authenticate(baseUri, login, password);

            // Checks
            assertNotNull(session);
            assertEquals("auth-token", session.getToken());
        }
    }
}
