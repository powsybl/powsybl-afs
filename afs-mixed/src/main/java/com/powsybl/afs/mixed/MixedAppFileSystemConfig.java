/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mixed;

import com.powsybl.afs.storage.AbstractAppFileSystemConfig;
import com.powsybl.commons.config.PlatformConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class MixedAppFileSystemConfig extends AbstractAppFileSystemConfig<MixedAppFileSystemConfig> {

    private final String nodeStorageDriveName;
    private final String dataStorageDriveName;

    static List<MixedAppFileSystemConfig> load() {
        return load(PlatformConfig.defaultConfig());
    }

    static List<MixedAppFileSystemConfig> load(PlatformConfig config) {
        return config.getOptionalModuleConfig("mixed-app-file-system").map(moduleConfig -> {
            List<MixedAppFileSystemConfig> configs = new ArrayList<>();
            final String driveName = moduleConfig.getStringProperty("drive-name");
            final String nodeStorage = moduleConfig.getStringProperty("node-storage");
            final String dataStorage = moduleConfig.getStringProperty("data-storage");
            // TODO remote access
            final MixedAppFileSystemConfig c = new MixedAppFileSystemConfig(driveName, true, nodeStorage, dataStorage);
            configs.add(c);
            return configs;
        }).orElse(Collections.emptyList());
    }

    public MixedAppFileSystemConfig(String driveName, boolean remotelyAccessible, String nodeStorageDriveName, String dataStorageDriveName) {
        super(driveName, remotelyAccessible);
        this.nodeStorageDriveName = Objects.requireNonNull(nodeStorageDriveName);
        this.dataStorageDriveName = Objects.requireNonNull(dataStorageDriveName);
    }

    public String getNodeStorageDriveName() {
        return nodeStorageDriveName;
    }

    public String getDataStorageDriveName() {
        return dataStorageDriveName;
    }

}
