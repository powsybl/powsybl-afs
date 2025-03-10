/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.storage;

import com.powsybl.commons.PowsyblException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
public abstract class AbstractExceptionTest<T extends PowsyblException> {
    protected static final String MESSAGE = "Test error MESSAGE";
    protected static final Throwable CAUSE = new Throwable("Root CAUSE");

    @Test
    void testDefaultConstructor() {
        T exception = createException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testMessageConstructor() {
        T exception = createExceptionWithMessage();
        assertEquals(MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testCauseConstructor() {
        T exception = createExceptionWithCause();
        assertEquals(CAUSE, exception.getCause());
    }

    @Test
    void testMessageAndCauseConstructor() {
        T exception = createExceptionWithMessageAndCause();
        assertEquals(MESSAGE, exception.getMessage());
        assertEquals(CAUSE, exception.getCause());
    }

    protected abstract T createException();

    protected abstract T createExceptionWithMessage();

    protected abstract T createExceptionWithCause();

    protected abstract T createExceptionWithMessageAndCause();
}
