package com.powsybl.afs.timeseriesserver;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.mapdb.MapDbAppFileSystemConfig;
import com.powsybl.commons.config.InMemoryPlatformConfig;
import com.powsybl.commons.config.MapModuleConfig;
import com.powsybl.commons.config.PlatformConfig;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TSServerAppFileSystemConfigTest {

    private static final String APP = "test-app";
    private static final String PORT = "8765";
    private static final String HOST = "localhost";
    private static final String SCHEME = "http";
    private static final String REMOTELY_ACCESSIBLE = "false";
    private static final String DRIVE_NAME = "test-drive";
    private static final String DB_FILE = "/db/test.db";

    private FileSystem fileSystem;
    private PlatformConfig platformConfig;

    @Before
    public void setup() {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        InMemoryPlatformConfig conf = new InMemoryPlatformConfig(fileSystem);
        final MapModuleConfig tsServerConfig = conf.createModuleConfig("tsserver-app-file-system");
        tsServerConfig.setStringProperty("drive-name", DRIVE_NAME);
        tsServerConfig.setStringProperty("remotely-accessible", REMOTELY_ACCESSIBLE);
        tsServerConfig.setStringProperty("scheme", SCHEME);
        tsServerConfig.setStringProperty("host", HOST);
        tsServerConfig.setStringProperty("port", PORT);
        tsServerConfig.setStringProperty("app", APP);
        final MapModuleConfig mapdbConfig = conf.createModuleConfig("delegate-mapdb-app-file-system");
        mapdbConfig.setStringProperty("drive-name", DRIVE_NAME);
        mapdbConfig.setPathProperty("db-file", fileSystem.getPath(DB_FILE));
        mapdbConfig.setStringProperty("remotely-accessible", REMOTELY_ACCESSIBLE);
        platformConfig = conf;
    }

    @Test
    public void loadTest() throws URISyntaxException {
        final TSServerAppFileSystemConfig conf = TSServerAppFileSystemConfig.load(platformConfig);
        assertEquals(conf.getApp(), APP);
        assertEquals(conf.getDriveName(), DRIVE_NAME);
        final URI uri = conf.getURI();
        assertEquals(uri.getScheme(), SCHEME);
        assertEquals(uri.getPort(), Integer.parseInt(PORT));
        assertEquals(uri.getHost(), HOST);
        assertEquals(conf.isRemotelyAccessible(), Boolean.valueOf(REMOTELY_ACCESSIBLE));

        final MapDbAppFileSystemConfig delegateConf = conf.getDelegateConfig();
        assertEquals(delegateConf.getDriveName(), DRIVE_NAME);
        assertEquals(delegateConf.isRemotelyAccessible(), Boolean.valueOf(REMOTELY_ACCESSIBLE));
        assertEquals(fileSystem.getPath(DB_FILE), delegateConf.getDbFile());
    }

    /**
     * Refer to config.yaml file in test resources
     */
    @Test
    public void emptyLoadTest() throws URISyntaxException {
        final TSServerAppFileSystemConfig load = TSServerAppFileSystemConfig.load();
        assertEquals(load.getDriveName(), "test-fs");
        assertFalse(load.isRemotelyAccessible());
        assertEquals(load.getApp(), "AFS");
        final URI uri = load.getURI();
        assertEquals(uri.getScheme(), "http");
        assertEquals(uri.getHost(), "localhost");
        assertEquals(uri.getPort(), 8080);
        final MapDbAppFileSystemConfig delegateConfig = load.getDelegateConfig();
        assertEquals(delegateConfig.getDriveName(), "test-fs");
        assertFalse(delegateConfig.isRemotelyAccessible());
    }
}
