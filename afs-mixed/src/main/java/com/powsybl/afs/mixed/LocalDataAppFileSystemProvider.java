/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mixed;

import com.google.auto.service.AutoService;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProvider;
import com.powsybl.afs.AppFileSystemProviderContext;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@AutoService(AppFileSystemProvider.class)
public class LocalDataAppFileSystemProvider implements AppFileSystemProvider {

    private final List<LocalDataAppFileSystemConfig> configs;

    public LocalDataAppFileSystemProvider() {
        this(LocalDataAppFileSystemConfig.load());
    }

    public LocalDataAppFileSystemProvider(List<LocalDataAppFileSystemConfig> configs) {
        this.configs = Objects.requireNonNull(configs);
    }

    @Override
    public List<AppFileSystem> getFileSystems(AppFileSystemProviderContext context) {
        return configs.stream().map(LocalDataAppFileSystem::new).collect(Collectors.toList());
    }
}
