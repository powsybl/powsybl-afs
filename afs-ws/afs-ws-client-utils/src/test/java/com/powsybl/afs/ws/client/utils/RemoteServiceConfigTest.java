/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.client.utils;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.commons.config.InMemoryPlatformConfig;
import com.powsybl.commons.config.MapModuleConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystem;

import static org.junit.Assert.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class RemoteServiceConfigTest {

    private FileSystem fileSystem;

    @Before
    public void createFileSystem() {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
    }

    @After
    public void closeFileSystem() throws IOException {
        fileSystem.close();
    }

    @Test
    public void test() {
        RemoteServiceConfig config = new RemoteServiceConfig("host", "test", 443, true);
        assertEquals("https://host:443/test", config.getRestUri().toString());
        assertEquals("wss://host:443/test", config.getWsUri().toString());

        RemoteServiceConfig config2 = new RemoteServiceConfig("host", "test", 80, false);
        assertEquals("http://host:80/test", config2.getRestUri().toString());
        assertEquals("ws://host:80/test", config2.getWsUri().toString());
    }

    @Test
    public void string() {
        RemoteServiceConfig config = new RemoteServiceConfig("host", "test", 443, true);
        assertEquals("RemoteServiceConfig{hostName=host, appName=test, port=443, secure=true, autoReconnectionEnabled=false, reconnectionDelay=60}", config.toString());
    }

    @Test
    public void readFromPlatformConfig() {
        InMemoryPlatformConfig platformConfig = new InMemoryPlatformConfig(fileSystem);

        MapModuleConfig moduleConfig = platformConfig.createModuleConfig("remote-service");
        moduleConfig.setStringProperty("host-name", "host");
        moduleConfig.setStringProperty("app-name", "app");
        moduleConfig.setStringProperty("auto-reconnection", "true");
        moduleConfig.setStringProperty("reconnection-delay", "5142");

        RemoteServiceConfig config = RemoteServiceConfig.load(platformConfig)
                .orElseThrow(AssertionError::new);

        assertEquals("https://host:443/app", config.getRestUri().toString());
        assertTrue(config.isAutoReconnectionEnabled());
        assertEquals(5142L, config.getReconnectionDelay());
    }
}
