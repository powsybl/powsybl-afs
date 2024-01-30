/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server.utils;

import com.powsybl.afs.ws.utils.ExceptionDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@Provider
public class ThrowableExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrowableExceptionMapper.class);

    @Override
    public Response toResponse(Throwable t) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error(t.toString(), t);
        }
        if (t instanceof WebApplicationException webApplicationException) {
            return webApplicationException.getResponse();
        } else {
            return Response
                    .serverError()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ExceptionDetail(t.getClass().getCanonicalName(), t.getMessage()))
                    .build();
        }
    }
}
