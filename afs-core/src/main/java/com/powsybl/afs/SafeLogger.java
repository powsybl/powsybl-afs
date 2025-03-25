/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs;

import org.owasp.encoder.Encode;
import org.slf4j.Logger;

import java.util.Arrays;

/**
 * Logger based on slf4j where all String inputs are sanitized
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
public class SafeLogger {
    private final Logger logger;

    public SafeLogger(Logger logger) {
        this.logger = logger;
    }

    public void error(String message, Object... args) {
        logger.error(message, sanitizeInputs(args));
    }

    public void warn(String message, Object... args) {
        logger.warn(message, sanitizeInputs(args));
    }

    public void debug(String message, Object... args) {
        logger.debug(message, sanitizeInputs(args));
    }

    public void info(String message, Object... args) {
        logger.info(message, sanitizeInputs(args));
    }

    public void trace(String message, Object... args) {
        logger.trace(message, sanitizeInputs(args));
    }

    private Object[] sanitizeInputs(Object... args) {
        return Arrays.stream(args)
            .map(this::sanitizeObject)
            .toArray();
    }

    private Object sanitizeObject(Object arg) {
        if (arg instanceof String string) {
            return sanitizeInput(string);
        }
        return arg;
    }

    private String sanitizeInput(String input) {
        if (input == null) {
            return null;
        }
        return Encode.forJava(input.replaceAll("[\r\n\t]", "_"));
    }
}

