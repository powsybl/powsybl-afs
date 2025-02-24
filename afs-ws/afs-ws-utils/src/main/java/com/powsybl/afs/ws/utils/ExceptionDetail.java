/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ws.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Paul Bui-Quang {@literal <paul.buiquang at rte-france.com>}
 */
public record ExceptionDetail(String javaException, String message) {

    public ExceptionDetail(@JsonProperty("javaException") String javaException, @JsonProperty("message") String message) {
        this.javaException = javaException;
        this.message = message;
    }
}
