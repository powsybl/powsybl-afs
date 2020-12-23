/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.jwt;

import com.powsybl.afs.ws.server.utils.KeyGenerator;
import com.powsybl.afs.ws.server.utils.SecurityConfig;
import com.powsybl.commons.config.PlatformConfig;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.client.HttpClientErrorException;

import javax.inject.Inject;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Key;

/**
 * @author Pratheep Thiyagarasa <pratheep.thiyagarasa at rte-france.com>
 */
public class JwtTokenFilter implements Filter {

    private static final Logger LOGGER = LoggerFactory.getLogger(JwtTokenFilter.class);

    @Inject
    private KeyGenerator keyGenerator;

    private boolean skipTokenValidityCheck;

    public JwtTokenFilter() {
        this(PlatformConfig.defaultConfig());
    }

    public JwtTokenFilter(PlatformConfig platformConfig) {
        skipTokenValidityCheck = SecurityConfig.load(platformConfig).isSkipTokenValidityCheck();
    }

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        if (!skipTokenValidityCheck) {
            HttpServletRequest request = (HttpServletRequest) servletRequest;
            // Get the HTTP Authorization header from the request
            String authorizationHeader = request.getHeader(HttpHeaders.AUTHORIZATION);
            LOGGER.trace("#### authorizationHeader : {}", authorizationHeader);

            // Check if the HTTP Authorization header is present and formatted correctly
            if (authorizationHeader == null || !authorizationHeader.startsWith("Bearer ")) {
                throw new HttpClientErrorException(HttpStatus.UNAUTHORIZED, "Authorization header must be provided", null, null);
            }

            // Extract the token from the HTTP Authorization header
            String token = authorizationHeader.substring("Bearer".length()).trim();

            // Validate the token
            Key key = keyGenerator.generateKey();
            Jws<Claims> claimsJws = Jwts.parser().setSigningKey(key).parseClaimsJws(token);
            SecurityContextHolder.getContext().setAuthentication(new AfsSecurityAuthentication(claimsJws.getBody().getSubject()));
        }
        filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() {
    }
}
