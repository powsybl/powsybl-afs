/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server;

import com.powsybl.afs.AppData;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.storage.AfsFileSystemNotFoundException;
import com.powsybl.afs.storage.AppStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Wrapper around {@link AppData} which provides additional checks around access to storage and filesystems.
 *
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@Component
public class AppDataWrapper {

    private final AppData appData;

    @Autowired
    public AppDataWrapper(AppData appData) {
        this.appData = appData;
    }

    public AppData getAppData() {
        return appData;
    }

    public AppStorage getStorage(String fileSystemName) {
        AppStorage storage = appData.getRemotelyAccessibleStorage(fileSystemName);
        if (storage == null) {
            throw new AfsFileSystemNotFoundException("App file system '" + fileSystemName + "' not found");
        }
        return storage;
    }

    public AppFileSystem getFileSystem(String name) {
        Objects.requireNonNull(appData);
        Objects.requireNonNull(name);
        AppFileSystem fileSystem = appData.getFileSystem(name);
        if (fileSystem == null) {
            throw new AfsFileSystemNotFoundException("App file system '" + name + "' not found");
        }
        return fileSystem;
    }
}
