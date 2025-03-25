/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ws.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class ExceptionDetailTest {

    @Test
    void testRecordInitialization() {
        String expectedException = "java.lang.NullPointerException";
        String expectedMessage = "Null pointer exception occurred";

        ExceptionDetail detail = new ExceptionDetail(expectedException, expectedMessage);

        assertEquals(expectedException, detail.javaException());
        assertEquals(expectedMessage, detail.message());
    }

    @Test
    void testJsonPropertyAnnotations() throws NoSuchMethodException {
        var constructor = ExceptionDetail.class.getConstructor(String.class, String.class);
        var params = constructor.getParameters();

        assertEquals("javaException", params[0].getAnnotation(JsonProperty.class).value());
        assertEquals("message", params[1].getAnnotation(JsonProperty.class).value());
    }
}
