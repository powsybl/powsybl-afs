/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.local;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.AfsException;
import com.powsybl.commons.config.InMemoryPlatformConfig;
import com.powsybl.commons.config.MapModuleConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class LocalAppFileSystemConfigTest {

    private FileSystem fileSystem;

    private InMemoryPlatformConfig platformConfig;

    @BeforeEach
    void setUp() throws Exception {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        Files.createDirectories(fileSystem.getPath("/tmp"));
        Files.createFile(fileSystem.getPath("/test"));
        platformConfig = new InMemoryPlatformConfig(fileSystem);
        MapModuleConfig moduleConfig = platformConfig.createModuleConfig("local-app-file-system");
        // Config 1
        moduleConfig.setStringProperty("drive-name", "local");
        moduleConfig.setPathProperty("root-dir", fileSystem.getPath("/work"));
        moduleConfig.setStringProperty("max-additional-drive-count", "2");

        // Config 2
        moduleConfig.setStringProperty("drive-name-1", "local1");
        moduleConfig.setStringProperty("remotely-accessible-1", "true");
        moduleConfig.setPathProperty("root-dir-1", fileSystem.getPath("/work"));
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.close();
    }

    @Test
    void loadTest() {
        List<LocalAppFileSystemConfig> configs = LocalAppFileSystemConfig.load(platformConfig);

        // Number of configs
        assertEquals(2, configs.size());
        LocalAppFileSystemConfig config1 = configs.get(0);
        LocalAppFileSystemConfig config2 = configs.get(1);

        // Checks on config 1
        assertEquals("local", config1.getDriveName());
        assertFalse(config1.isRemotelyAccessible());
        assertEquals(fileSystem.getPath("/work"), config1.getRootDir());

        // Check on config 2
        assertEquals("local1", config2.getDriveName());
        assertTrue(config2.isRemotelyAccessible());
        assertEquals(fileSystem.getPath("/work"), config2.getRootDir());
    }

    @Test
    void configModificationTest() {
        List<LocalAppFileSystemConfig> configs = LocalAppFileSystemConfig.load(platformConfig);
        LocalAppFileSystemConfig config = configs.get(0);

        // Change some configuration parameters
        config.setDriveName("local2");
        config.setRootDir(fileSystem.getPath("/tmp"));

        // Check that the parameters are updated
        assertEquals("local2", config.getDriveName());
        assertEquals(fileSystem.getPath("/tmp"), config.getRootDir());
    }

    @Test
    void setRootDirToFileExceptionTest() {
        List<LocalAppFileSystemConfig> configs = LocalAppFileSystemConfig.load(platformConfig);
        LocalAppFileSystemConfig config = configs.get(0);

        // Try to set rootDir to a file instead of a directory
        Path filePath = fileSystem.getPath("/test");
        AfsException exception = assertThrows(AfsException.class, () -> config.setRootDir(filePath));
        assertEquals("Root path /test is not a directory", exception.getMessage());
    }
}
