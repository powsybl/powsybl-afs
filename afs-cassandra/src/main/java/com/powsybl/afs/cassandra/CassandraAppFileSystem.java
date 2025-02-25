/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.storage.AppStorage;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class CassandraAppFileSystem extends AppFileSystem {

    public CassandraAppFileSystem(String name, boolean remotelyAccessible, AppStorage storage) {
        super(name, remotelyAccessible, storage);
    }
}
