/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class AfsCircularDependencyExceptionTest {

    @Test
    void testDefaultConstructor() {
        AfsCircularDependencyException exception = new AfsCircularDependencyException();
        assertEquals("Circular dependency detected", exception.getMessage());
        assertNull(exception.getCause());
    }
}
