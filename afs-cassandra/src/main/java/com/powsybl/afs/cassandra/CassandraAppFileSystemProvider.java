/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.auto.service.AutoService;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProvider;
import com.powsybl.afs.AppFileSystemProviderContext;
import com.powsybl.afs.storage.EventsBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@AutoService(AppFileSystemProvider.class)
public class CassandraAppFileSystemProvider implements AppFileSystemProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAppFileSystemProvider.class);

    private final List<CassandraAppFileSystemConfig> fileSystemConfigs;

    private final CassandraAppStorageConfig storageConfig;

    public CassandraAppFileSystemProvider() {
        this(CassandraAppFileSystemConfig.load(), CassandraAppStorageConfig.load());
    }

    public CassandraAppFileSystemProvider(List<CassandraAppFileSystemConfig> fileSystemConfigs,
                                          CassandraAppStorageConfig storageConfig) {
        this.fileSystemConfigs = Objects.requireNonNull(fileSystemConfigs);
        this.storageConfig = Objects.requireNonNull(storageConfig);
    }

    private static CassandraAppFileSystem createFileSystem(CassandraAppFileSystemConfig fileSystemConfig,
                                                           CassandraAppStorageConfig storageConfig, EventsBus eventsBus) {
        try {
            return new CassandraAppFileSystem(fileSystemConfig.getDriveName(),
                                              fileSystemConfig.isRemotelyAccessible(),
                                              new CassandraAppStorage(fileSystemConfig.getDriveName(),
                                                  () -> new CassandraSimpleContext(fileSystemConfig.getIpAddresses().stream().distinct().collect(Collectors.toList()),
                                                                                   fileSystemConfig.getLocalDc()),
                                                  storageConfig, eventsBus));
        } catch (NoHostAvailableException e) {
            LOGGER.error(e.toString(), e);
            return null;
        }
    }

    @Override
    public List<AppFileSystem> getFileSystems(AppFileSystemProviderContext context) {
        return fileSystemConfigs.stream()
                .map(fileSystemConfig -> createFileSystem(fileSystemConfig, storageConfig, context.getEventsBus()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
