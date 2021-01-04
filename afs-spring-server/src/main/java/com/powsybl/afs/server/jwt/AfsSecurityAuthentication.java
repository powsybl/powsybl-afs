/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.server.jwt;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;
import java.util.Collection;
import java.util.Objects;

/**
 * @author Pratheep Thiyagarasa <pratheep.thiyagarasa at rte-france.com>
 */
public class AfsSecurityAuthentication implements Authentication {

    private final Principal user;

    public AfsSecurityAuthentication(String username) {
        Objects.requireNonNull(username);
        this.user = new UserPrincipal() {
            @Override
            public String getName() {
                return username;
            }

            @Override
            public int hashCode() {
                return getName().hashCode();
            }

            @Override
            public boolean equals(Object obj) {
                if (obj instanceof UserPrincipal) {
                    return getName().equals(((UserPrincipal) obj).getName());
                }
                return false;
            }
        };
    }

    @Override
    public String getName() {
        return user.getName();
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return null;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getDetails() {
        return null;
    }

    @Override
    public Object getPrincipal() {
        return user;
    }

    @Override
    public boolean isAuthenticated() {
        return true;
    }

    @Override
    public void setAuthenticated(boolean b) throws IllegalArgumentException {
    }
}
