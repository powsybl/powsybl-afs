/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server;

import com.powsybl.afs.ws.server.utils.KeyGenerator;
import com.powsybl.afs.ws.server.utils.SecurityConfig;
import com.powsybl.afs.ws.server.utils.UserAuthenticator;
import com.powsybl.commons.config.PlatformConfig;
import com.powsybl.commons.net.UserProfile;
import io.jsonwebtoken.Jwts;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.security.Key;
import java.time.ZonedDateTime;
import java.util.Date;

import static jakarta.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static jakarta.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;

/**
 * @author Ali Tahanout {@literal <ali.tahanout at rte-france.com>}
 */
@Path("/users")
public class UserEndpoint {

    @Context
    private UriInfo uriInfo;

    @Inject
    private KeyGenerator keyGenerator;

    @Inject
    private UserAuthenticator authenticator;

    private final long tokenValidity;

    public UserEndpoint() {
        this(PlatformConfig.defaultConfig());
    }

    public UserEndpoint(PlatformConfig platformConfig) {
        tokenValidity = SecurityConfig.load(platformConfig).getTokenValidity();
    }

    @POST
    @Path("/login")
    @Consumes(APPLICATION_FORM_URLENCODED)
    @Produces(APPLICATION_JSON)
    public Response authenticateUser(@FormParam("login") String login, @FormParam("password") String password) {
        try {
            UserProfile profile = authenticator.check(login, password);
            String token = issueToken(login);
            return Response.ok().header(AUTHORIZATION, "Bearer " + token).entity(profile).build();
        } catch (SecurityException e) {
            return Response.status(UNAUTHORIZED).build();
        }
    }

    private String issueToken(String login) {
        Key key = keyGenerator.generateKey();
        ZonedDateTime now = ZonedDateTime.now();
        return Jwts.builder()
            .signWith(key)
            .compressWith(Jwts.ZIP.DEF)
            .subject(login)
            .issuer(uriInfo.getAbsolutePath().toString())
            .issuedAt(Date.from(now.toInstant()))
            .expiration(Date.from(now.plusMinutes(tokenValidity).toInstant()))
            .compact();
    }
}
