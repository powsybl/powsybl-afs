/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.google.auto.service.AutoService;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProvider;
import com.powsybl.afs.AppFileSystemProviderContext;

import java.util.Collections;
import java.util.List;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@AutoService(AppFileSystemProvider.class)
public class PostgresAppFileSystemAppProvider implements AppFileSystemProvider {

    @Override
    public List<AppFileSystem> getFileSystems(AppFileSystemProviderContext context) {
        return Collections.emptyList();
//        return Collections.singletonList(new PostgresAppFileSystem("test", true, new PostgresAppStorage()));
    }
}
