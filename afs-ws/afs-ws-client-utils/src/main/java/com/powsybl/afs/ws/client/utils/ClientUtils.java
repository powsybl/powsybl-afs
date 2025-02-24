/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.client.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.ws.utils.ExceptionDetail;
import com.powsybl.afs.ws.utils.JsonProvider;
import com.powsybl.commons.net.UserProfile;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Form;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public final class ClientUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientUtils.class);

    private ClientUtils() {
    }

    public static Client createClient() {
        return new ResteasyClientBuilderImpl()
                .connectionPoolSize(50)
                .build();
    }

    private static RuntimeException createServerErrorException(Response response) {
        String body = response.readEntity(String.class);
        try {
            ExceptionDetail exceptionDetail = new ObjectMapper().readValue(body, ExceptionDetail.class);
            String javaException = exceptionDetail.javaException();
            if (javaException != null) {
                Class<?> exceptionClass = Class.forName(javaException);
                if (RuntimeException.class.isAssignableFrom(exceptionClass)) {
                    if (exceptionDetail.message() != null) {
                        return (RuntimeException) exceptionClass.getConstructor(String.class).newInstance(exceptionDetail.message());
                    }
                    return (RuntimeException) exceptionClass.getDeclaredConstructor().newInstance();
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to handle registered exception", e);
        }
        return new AfsStorageException(body);
    }

    private static AfsStorageException createUnexpectedResponseStatus(Response.Status status) {
        return new AfsStorageException("Unexpected response status: '" + status + "'");
    }

    public static void checkOk(Response response) {
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        if (status != Response.Status.OK) {
            if (status == Response.Status.INTERNAL_SERVER_ERROR) {
                throw createServerErrorException(response);
            } else {
                throw createUnexpectedResponseStatus(status);
            }
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
        } else if (status == Response.Status.INTERNAL_SERVER_ERROR) {
            throw createServerErrorException(response);
        } else {
            throw createUnexpectedResponseStatus(status);
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
        } else if (status == Response.Status.INTERNAL_SERVER_ERROR) {
            throw createServerErrorException(response);
        } else {
            throw createUnexpectedResponseStatus(status);
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

        Client client = ClientUtils.createClient()
                .register(new JsonProvider());
        try {
            Form form = new Form()
                    .param("login", login)
                    .param("password", password);

            Response response = client.target(baseUri)
                    .path("rest")
                    .path("users")
                    .path("login")
                    .request()
                    .post(Entity.form(form));
            try {
                UserProfile profile = readEntityIfOk(response, UserProfile.class);
                String token = response.getHeaderString(HttpHeaders.AUTHORIZATION);
                return new UserSession(profile, token);
            } finally {
                response.close();
            }
        } finally {
            client.close();
        }
    }

}
