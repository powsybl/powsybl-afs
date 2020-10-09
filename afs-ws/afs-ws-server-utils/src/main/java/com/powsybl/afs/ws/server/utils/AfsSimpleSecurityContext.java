/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ws.server.utils;

import javax.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.util.Objects;

/**
 * @author Paul Bui-Quang <paul.buiquang at rte-france.com>
 */
public class AfsSimpleSecurityContext implements SecurityContext {

    private Principal user;

    public AfsSimpleSecurityContext(String username) {
        this.user = new Principal() {
            @Override
            public String getName() {
                return username;
            }
        };
    }

    @Override
    public Principal getUserPrincipal() {
        return user;
    }

    @Override
    public boolean isUserInRole(String role) {
        return false;
    }

    @Override
    public boolean isSecure() {
        return true;
    }

    @Override
    public String getAuthenticationScheme() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AfsSimpleSecurityContext that = (AfsSimpleSecurityContext) o;
        return Objects.equals(user.getName(), that.user.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(user.getName());
    }
}
