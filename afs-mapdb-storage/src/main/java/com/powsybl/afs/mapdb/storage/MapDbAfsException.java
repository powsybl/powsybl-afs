/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mapdb.storage;

import com.powsybl.commons.PowsyblException;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
public class MapDbAfsException extends PowsyblException {

    public MapDbAfsException() {
    }

    public MapDbAfsException(String message) {
        super(message);
    }

    public MapDbAfsException(Throwable throwable) {
        super(throwable);
    }

    public MapDbAfsException(String message, Throwable cause) {
        super(message, cause);
    }
}
