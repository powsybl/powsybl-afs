/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server.utils;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Paul Bui-Quang {@literal <paul.buiquang at rte-france.com>}
 */
public class AfsSimpleSecurityContextTest {

    @Test
    public void equalityTest() {
        AfsSimpleSecurityContext afsSimpleSecurityContext = new AfsSimpleSecurityContext("foo");
        assertThat(afsSimpleSecurityContext.getUserPrincipal().getName()).isEqualTo("foo");
        assertThat(afsSimpleSecurityContext)
                .isEqualTo(afsSimpleSecurityContext)
                .isEqualTo(new AfsSimpleSecurityContext("foo"))
                .isNotEqualTo(new AfsSimpleSecurityContext("bar"))
                .isNotEqualTo(null)
                .hasSameHashCodeAs(new AfsSimpleSecurityContext("foo"));

        assertThat(afsSimpleSecurityContext.getAuthenticationScheme()).isNull();
        assertThat(afsSimpleSecurityContext.isSecure()).isTrue();
        assertThat(afsSimpleSecurityContext.isUserInRole("")).isFalse();
    }

}
