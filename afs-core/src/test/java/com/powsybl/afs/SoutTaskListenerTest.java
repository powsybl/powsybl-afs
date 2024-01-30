/*
 * Copyright (c) 2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class SoutTaskListenerTest {

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();

    @BeforeEach
    public void setUp() {
        System.setOut(new PrintStream(outputStreamCaptor));
    }

    @AfterEach
    public void tearDown() {
        System.setOut(standardOut);
    }

    @Test
    void testUpdateTaskMessageEvent() {
        SoutTaskListener listener = new SoutTaskListener(System.out);
        TaskEvent event = new UpdateTaskMessageEvent(new UUID(0L, 0L), 0L, "event message");
        listener.onEvent(event);
        assertEquals("event message", outputStreamCaptor.toString().trim());
    }

    @Test
    void testOtherEvent() {
        SoutTaskListener listener = new SoutTaskListener(System.out);
        TaskEvent otherEvent = new StartTaskEvent(new UUID(0L, 0L), 0L, "event message");
        listener.onEvent(otherEvent);
        assertEquals("", outputStreamCaptor.toString().trim());
    }
}
