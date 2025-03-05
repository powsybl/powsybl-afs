/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import com.powsybl.commons.PowsyblException;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class AfsStorageException extends PowsyblException {

    public AfsStorageException() {
        super();
    }

    public AfsStorageException(String message) {
        super(message);
    }

    public AfsStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
