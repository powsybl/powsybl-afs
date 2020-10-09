/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server.utils;

import org.junit.Test;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mathieu Bague <mathieu.bague at rte-france.com>
 */
public class CodeCoverageTest {

    @Test
    public void testNothing() {
        // TODO: remove this class once a real test has been added
        AfsSimpleSecurityContext afsSimpleSecurityContext = new AfsSimpleSecurityContext("foo");
        AfsSimpleSecurityContext identity = afsSimpleSecurityContext;
        assertThat(afsSimpleSecurityContext.getUserPrincipal().getName()).isEqualTo("foo");
        assertThat(afsSimpleSecurityContext).isEqualTo(new AfsSimpleSecurityContext("foo"));
        assertThat(afsSimpleSecurityContext).isEqualTo(identity);
        assertThat(afsSimpleSecurityContext).isNotEqualTo(null);
        assertThat(afsSimpleSecurityContext.hashCode()).isEqualTo(Objects.hash("foo"));
        assertThat(afsSimpleSecurityContext.getAuthenticationScheme()).isNull();
        assertThat(afsSimpleSecurityContext.isSecure()).isTrue();
        assertThat(afsSimpleSecurityContext.isUserInRole("")).isFalse();
    }
}
