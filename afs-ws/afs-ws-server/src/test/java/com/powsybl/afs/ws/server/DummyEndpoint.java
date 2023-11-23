/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ws.server;

import org.apache.commons.lang3.NotImplementedException;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.concurrent.CancellationException;

/**
 * @author Paul Bui-Quang {@literal <paul.buiquang at rte-france.com>}
 */
@Named
@ApplicationScoped
@Path("dummy")
public class DummyEndpoint {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registeredError")
    public Response registeredError() {
        throw new CancellationException();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("unregisteredError")
    public Response unregisteredError() throws IOException {
        throw new IOException();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registeredErrorWithMessage")
    public Response registeredErrorWithMessage() {
        throw new NotImplementedException("hello");
    }
}
