/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mixed;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.commons.config.InMemoryPlatformConfig;
import com.powsybl.commons.config.MapModuleConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class MixedAppFileSystemConfigTest {

    private FileSystem fileSystem;

    private InMemoryPlatformConfig platformConfig;

    @Before
    public void setUp() throws Exception {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        Files.createDirectories(fileSystem.getPath("/tmp"));
        Files.createFile(fileSystem.getPath("/test"));
        platformConfig = new InMemoryPlatformConfig(fileSystem);
        MapModuleConfig moduleConfig = platformConfig.createModuleConfig("mixed-app-file-system");
        moduleConfig.setStringProperty("drive-name", "mixed");
        moduleConfig.setStringProperty("node-storage", "n");
        moduleConfig.setStringProperty("data-storage", "d");
    }

    @After
    public void tearDown() throws Exception {
        fileSystem.close();
    }

    @Test
    public void testLoad() {
        final List<MixedAppFileSystemConfig> configs = MixedAppFileSystemConfig.load(platformConfig);
        assertEquals(1, configs.size());
        final MixedAppFileSystemConfig config = configs.get(0);
        assertEquals("mixed", config.getDriveName());
        assertEquals("n", config.getNodeStorageDriveName());
        assertEquals("d", config.getDataStorageDriveName());
    }
}
