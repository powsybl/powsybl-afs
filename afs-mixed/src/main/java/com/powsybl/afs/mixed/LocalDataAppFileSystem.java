/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mixed;

import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.mixed.storage.LocalDataAppStorage;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class LocalDataAppFileSystem extends AppFileSystem {

    LocalDataAppFileSystem(LocalDataAppFileSystemConfig config) {
        super(config.getDriveName(), config.isRemotelyAccessible(), new LocalDataAppStorage(config));
    }
}
