/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.storage;

import com.powsybl.commons.PowsyblException;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
public class AfsNodeNotFoundException extends PowsyblException {

    public AfsNodeNotFoundException() {
    }

    public AfsNodeNotFoundException(String msg) {
        super(msg);
    }

    public AfsNodeNotFoundException(Throwable throwable) {
        super(throwable);
    }

    public AfsNodeNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
