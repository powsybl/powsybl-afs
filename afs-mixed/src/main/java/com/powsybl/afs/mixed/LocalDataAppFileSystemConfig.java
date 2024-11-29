/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mixed;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.storage.AbstractAppFileSystemConfig;
import com.powsybl.commons.config.ModuleConfig;
import com.powsybl.commons.config.PlatformConfig;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class LocalDataAppFileSystemConfig extends AbstractAppFileSystemConfig<LocalDataAppFileSystemConfig> {

    private static final String DRIVE_NAME = "drive-name";
    private static final String ROOT_DIR = "root-dir";

    private Path rootDir;

    public static List<LocalDataAppFileSystemConfig> load() {
        return load(PlatformConfig.defaultConfig());
    }

    private static void load(ModuleConfig moduleConfig, int num, List<LocalDataAppFileSystemConfig> configs) {
        StringBuilder driveNameTag = new StringBuilder(DRIVE_NAME);
        StringBuilder rootDirTag = new StringBuilder(ROOT_DIR);
        driveNameTag.append("-").append(num);
        rootDirTag.append("-").append(num);
        if (moduleConfig.hasProperty(driveNameTag.toString())
                && moduleConfig.hasProperty(rootDirTag.toString())) {
            String driveName = moduleConfig.getStringProperty(driveNameTag.toString());
            Path rootDir = moduleConfig.getPathProperty(rootDirTag.toString());
            configs.add(new LocalDataAppFileSystemConfig(driveName, rootDir));
        }
    }

    private static void load(ModuleConfig moduleConfig, List<LocalDataAppFileSystemConfig> configs) {
        if (moduleConfig.hasProperty(DRIVE_NAME)
                && moduleConfig.hasProperty(ROOT_DIR)) {
            String driveName = moduleConfig.getStringProperty(DRIVE_NAME);
            Path rootDir = moduleConfig.getPathProperty(ROOT_DIR);
            configs.add(new LocalDataAppFileSystemConfig(driveName, rootDir));
        }
    }

    public static List<LocalDataAppFileSystemConfig> load(PlatformConfig platformConfig) {
        return platformConfig.getOptionalModuleConfig("local-data-app-file-system")
                .map(moduleConfig -> {
                    List<LocalDataAppFileSystemConfig> configs = new ArrayList<>();
                    load(moduleConfig, configs);
                    int maxAdditionalDriveCount = moduleConfig.getIntProperty("max-additional-drive-count", 0);
                    for (int i = 0; i < maxAdditionalDriveCount; i++) {
                        load(moduleConfig, i, configs);
                    }
                    return configs;
                })
                .orElseGet(() -> {
                    List<LocalDataAppFileSystemConfig> configs = new ArrayList<>();
                    for (Path rootDir : FileSystems.getDefault().getRootDirectories()) {
                        if (Files.isDirectory(rootDir)) {
                            configs.add(new LocalDataAppFileSystemConfig(rootDir.toString(), rootDir));
                        }
                    }
                    return configs;
                });

    }

    private static Path checkRootDir(Path rootDir) {
        Objects.requireNonNull(rootDir);
        if (!Files.isDirectory(rootDir)) {
            throw new AfsException("Root path " + rootDir + " is not a directory");
        }
        return rootDir;
    }

    public LocalDataAppFileSystemConfig(String driveName, Path rootDir) {
        super(driveName, false);
        this.rootDir = checkRootDir(rootDir).toAbsolutePath();
    }

    public Path getRootDir() {
        return rootDir;
    }

    public LocalDataAppFileSystemConfig setRootDir(Path rootDir) {
        this.rootDir = checkRootDir(rootDir);
        return this;
    }
}
