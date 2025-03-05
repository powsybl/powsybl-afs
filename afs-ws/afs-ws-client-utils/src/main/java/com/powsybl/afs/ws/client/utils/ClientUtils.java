/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.client.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.ext.base.ScriptException;
import com.powsybl.afs.storage.AfsNodeNotFoundException;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.ws.utils.ExceptionDetail;
import com.powsybl.afs.ws.utils.JsonProvider;
import com.powsybl.commons.net.UserProfile;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Form;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public final class ClientUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientUtils.class);
    private static final Map<String, Class<? extends RuntimeException>> EXPECTED_EXCEPTION_CLASSES = Map.of(
        "ScriptException", ScriptException.class,
        "RuntimeException", RuntimeException.class,
        "AfsNodeNotFoundException", AfsNodeNotFoundException.class,
        "AfsStorageException", AfsStorageException.class,
        "AfsException", AfsException.class
    );

    private ClientUtils() {
    }

    public static Client createClient() {
        return new ResteasyClientBuilderImpl()
            .connectionPoolSize(50)
            .build();
    }

    private static RuntimeException createSpecificException(Response response, String... expectedExceptionClassNames) {
        String body = response.readEntity(String.class);
        try {
            ExceptionDetail exceptionDetail = new ObjectMapper().readValue(body, ExceptionDetail.class);
            String javaException = exceptionDetail.javaException();

            // Check if the exception is empty
            if (javaException == null) {
                throw new AfsStorageException("No exception was found in response");
            }

            // Create the exception from the response
            Class<?> exceptionClass = Class.forName(javaException);
            String message = exceptionDetail.message();
            Object instance = message != null ?
                exceptionClass.getDeclaredConstructor(String.class).newInstance(message) :
                exceptionClass.getDeclaredConstructor().newInstance();

            // Test the different provided classes
            for (String expectedExceptionClassName : expectedExceptionClassNames) {
                Class<? extends RuntimeException> expectedExceptionClass = EXPECTED_EXCEPTION_CLASSES.get(expectedExceptionClassName);
                if (expectedExceptionClass != null && expectedExceptionClass.isAssignableFrom(exceptionClass)) {
                    return expectedExceptionClass.cast(instance);
                }
            }

            // No corresponding class was found
            throw new AfsStorageException("No corresponding exception class was found in: "
                + String.join(", ", expectedExceptionClassNames));
        } catch (ReflectiveOperationException e) {
            throw new AfsStorageException("Reflexion exception: " + e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.warn("Failed to handle registered exception", e);
            throw new AfsStorageException("Failed to handle registered exception");
        }
    }

    private static AfsStorageException createUnexpectedResponseStatus(Response.Status status) {
        return new AfsStorageException("Unexpected response status: '" + status + "'");
    }

    private static RuntimeException createExceptionAccordingToResponse(Response response) {
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        if (status == Response.Status.INTERNAL_SERVER_ERROR) {
            return createSpecificException(response, "ScriptException", "AfsStorageException", "RuntimeException");
        } else if (status == Response.Status.NOT_FOUND) {
            return createSpecificException(response, "AfsNodeNotFoundException");
        } else if (status == Response.Status.BAD_REQUEST) {
            return createSpecificException(response, "AfsException");
        } else {
            return createUnexpectedResponseStatus(status);
        }
    }

    public static void checkOk(Response response) {
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        if (status != Response.Status.OK) {
            throw createExceptionAccordingToResponse(response);
        }
    }

    private static <T> T readEntityAndLog(Response response, Class<T> entityType) {
        T entity = response.readEntity(entityType);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("    --> {}", entity);
        }
        return entity;
    }

    public static <T> T readEntityIfOk(Response response, Class<T> entityType) {
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        if (status == Response.Status.OK) {
            return readEntityAndLog(response, entityType);
        } else {
            throw createExceptionAccordingToResponse(response);
        }
    }

    public static <T> T readEntityIfOk(Response response, GenericType<T> entityType) {
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        if (status == Response.Status.OK) {
            T entity = response.readEntity(entityType);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("    --> {}", entity);
            }
            return entity;
        } else {
            throw createExceptionAccordingToResponse(response);
        }

    }

    public static <T> Optional<T> readOptionalEntityIfOk(Response response, Class<T> entityType) {
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        // the NO_CONTENT case is for backwards compatibility.
        // The remote AppStorageServer may still runs on an old version which response with 204 if it not found resources.
        if (status == Response.Status.NO_CONTENT || status == Response.Status.NOT_FOUND) {
            LOGGER.trace("    --> null");
            return Optional.empty();
        }
        return Optional.of(readEntityIfOk(response, entityType));
    }

    public static UserSession authenticate(URI baseUri, String login, String password) {
        Objects.requireNonNull(baseUri);
        Objects.requireNonNull(login);
        Objects.requireNonNull(password);

        try (Client client = ClientUtils.createClient()
            .register(new JsonProvider())) {
            Form form = new Form()
                .param("login", login)
                .param("password", password);

            try (Response response = client.target(baseUri)
                .path("rest")
                .path("users")
                .path("login")
                .request()
                .post(Entity.form(form))) {
                UserProfile profile = readEntityIfOk(response, UserProfile.class);
                String token = response.getHeaderString(HttpHeaders.AUTHORIZATION);
                return new UserSession(profile, token);
            }
        }
    }

}
