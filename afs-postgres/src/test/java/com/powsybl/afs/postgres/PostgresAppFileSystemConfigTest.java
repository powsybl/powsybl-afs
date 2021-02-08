/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.commons.config.InMemoryPlatformConfig;
import com.powsybl.commons.config.MapModuleConfig;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class PostgresAppFileSystemConfigTest {

    @Test
    public void test() {
        try (FileSystem fileSystem = Jimfs.newFileSystem(Configuration.unix())) {
            InMemoryPlatformConfig platformConfig = new InMemoryPlatformConfig(fileSystem);
            MapModuleConfig moduleConfig = platformConfig.createModuleConfig("postgres-app-file-system");
            moduleConfig.setStringProperty("drive-name", "afs");
            moduleConfig.setStringProperty("ip-address", "localhost:5432");
            moduleConfig.setStringProperty("username", "username");
            moduleConfig.setStringProperty("password", "pwd");

            final List<PostgresAppFileSystemConfig> configs = PostgresAppFileSystemConfig.load(platformConfig);
            assertEquals(1, configs.size());
            final PostgresAppFileSystemConfig postgresAppFileSystemConfig = configs.get(0);
            assertEquals("username", postgresAppFileSystemConfig.getUsername());
            assertEquals("pwd", postgresAppFileSystemConfig.getPassword());
            assertEquals("localhost:5432", postgresAppFileSystemConfig.getIpAddress());
            assertEquals("afs", postgresAppFileSystemConfig.getDriveName());
        } catch (IOException e) {
            fail();
        }
    }
}
