/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mixed;

import com.powsybl.afs.AppData;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.mixed.storage.MixedAppStorage;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class MixedAppFileSystem extends AppFileSystem {

    MixedAppFileSystem(MixedAppFileSystemConfig config) {
        super(config.getDriveName(), config.isRemotelyAccessible(), new MixedAppStorage(config));
    }

    @Override
    protected void setData(AppData data) {
        super.setData(data);
        ((MixedAppStorage) storage).setAppData(data);
    }

}
