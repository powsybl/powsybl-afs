/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.client.utils;

import com.google.common.collect.Lists;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.ws.utils.JsonProvider;
import com.powsybl.afs.ws.utils.exceptions.RegisteredExceptionForwards;
import com.powsybl.commons.net.UserProfile;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public final class ClientUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientUtils.class);

    private static final Set<Class<? extends RuntimeException>> REGISTERED_EXCEPTIONS_FORWARDS = Lists
            .newArrayList(ServiceLoader.load(RegisteredExceptionForwards.class))
            .stream()
            .flatMap(registeredExceptionForwards -> registeredExceptionForwards.getExceptionClasses().stream())
            .collect(Collectors.toSet());

    private ClientUtils() {
    }

    public static Client createClient() {
        return new ResteasyClientBuilder()
                .connectionPoolSize(50)
                .build();
    }

    private static RuntimeException createServerErrorException(Response response) {
        try {
            String javaException = response.getHeaderString("java-exception");
            if (javaException != null) {
                Class exceptionClass = Class.forName(javaException);
                if (REGISTERED_EXCEPTIONS_FORWARDS.contains(exceptionClass)) {
                    return (RuntimeException) exceptionClass.newInstance();
                }
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOGGER.warn("Failed to handle registered exception", e);
        }
        return new AfsStorageException(response.readEntity(String.class));
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
        if (status == Response.Status.OK) {
            return Optional.of(readEntityAndLog(response, entityType));
        } else if (status == Response.Status.NO_CONTENT) {
            LOGGER.trace("    --> null");
            return Optional.empty();
        } else if (status == Response.Status.INTERNAL_SERVER_ERROR) {
            throw createServerErrorException(response);
        } else {
            throw createUnexpectedResponseStatus(status);
        }
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
