/*
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ws.server;

import com.google.common.collect.Lists;
import com.powsybl.afs.ws.utils.exceptions.RegisteredExceptionForwards;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Paul Bui-Quang <paul.buiquang at rte-france.com>
 */
@Provider
public class ExceptionHandler implements ExceptionMapper<Exception> {

    private static final Set<Class<? extends RuntimeException>> REGISTERED_EXCEPTIONS_FORWARDS = Lists
            .newArrayList(ServiceLoader.load(RegisteredExceptionForwards.class))
            .stream()
            .flatMap(registeredExceptionForwards -> registeredExceptionForwards.getExceptionClasses().stream())
            .collect(Collectors.toSet());

    @Override
    public Response toResponse(Exception exception) {
        Response.ResponseBuilder responseBuilder = Response.status(500);
        REGISTERED_EXCEPTIONS_FORWARDS
                .stream()
                .filter(exceptionClass -> exceptionClass.equals(exception.getClass()))
                .findFirst()
                .ifPresent(exceptionClass -> responseBuilder.header("java-exception", exceptionClass.getCanonicalName()));
        return responseBuilder.build();
    }
}
