package com.powsybl.afs.timeseriesserver;

import com.powsybl.afs.mapdb.MapDbAppFileSystemConfig;
import com.powsybl.afs.storage.AbstractAppFileSystemConfig;
import com.powsybl.commons.config.PlatformConfig;
import lombok.Getter;
import lombok.Setter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * App file system configuration for time series server app storage
 *
 * @author amichaut@artelys.com
 */
public class TSServerAppFileSystemConfig extends AbstractAppFileSystemConfig<TSServerAppFileSystemConfig> {

    private static final String TSSERVER_APP_FILE_SYSTEM = "tsserver-app-file-system";
    private static final String DRIVE_NAME = "drive-name";
    private static final String REMOTELY_ACCESSIBLE = "remotely-accessible";
    private static final boolean DEFAULT_REMOTELY_ACCESSIBLE = false;
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String SCHEME = "scheme";
    private static final String APP = "app";
    private static final String DEFAULT_APP = "AFS";
    public static final String DELEGATE_MAPDB_APP_FILE_SYSTEM = "delegate-mapdb-app-file-system";

    @Setter
    private String host;

    @Setter
    private int port;

    @Setter
    private String scheme;

    @Getter
    @Setter
    private String app = DEFAULT_APP;

    @Getter
    @Setter
    private MapDbAppFileSystemConfig delegateConfig;

    public TSServerAppFileSystemConfig(final String driveName, final boolean remotelyAccessible) {
        super(driveName, remotelyAccessible);
    }

    public static TSServerAppFileSystemConfig load() {
        return load(PlatformConfig.defaultConfig());
    }

    public static TSServerAppFileSystemConfig load(final PlatformConfig platformConfig) {
        return platformConfig.getOptionalModuleConfig(TSSERVER_APP_FILE_SYSTEM)
                             .map(moduleConfig -> {
                                 final String driveName;
                                 if (moduleConfig.hasProperty(DRIVE_NAME)) {
                                     driveName = moduleConfig.getStringProperty(DRIVE_NAME);
                                 } else {
                                     throw new IllegalArgumentException("Please provide a drive name for timeseries server app file system configuration");
                                 }
                                 final boolean remotelyAccessible = moduleConfig.getBooleanProperty(REMOTELY_ACCESSIBLE, DEFAULT_REMOTELY_ACCESSIBLE);
                                 final TSServerAppFileSystemConfig config = new TSServerAppFileSystemConfig(driveName, remotelyAccessible);
                                 if (moduleConfig.hasProperty(HOST)) {
                                     config.setHost(moduleConfig.getStringProperty(HOST));
                                 }
                                 if (moduleConfig.hasProperty(PORT)) {
                                     config.setPort(moduleConfig.getIntProperty(PORT));
                                 }
                                 if (moduleConfig.hasProperty(SCHEME)) {
                                     config.setScheme(moduleConfig.getStringProperty(SCHEME));
                                 }
                                 config.setApp(moduleConfig.getStringProperty(APP, DEFAULT_APP));
                                 // Load delegate config
                                 final List<MapDbAppFileSystemConfig> delegateConfig = MapDbAppFileSystemConfig.load(platformConfig.getModuleConfig(DELEGATE_MAPDB_APP_FILE_SYSTEM));
                                 if (delegateConfig.size() != 1) {
                                     throw new IllegalArgumentException("A single mapdb delegate configuration is necessary");
                                 }
                                 config.setDelegateConfig(delegateConfig.get(0));
                                 return config;
                             })
                             .orElse(null);
    }

    /**
     * @return target URI for timeseries service
     * @throws URISyntaxException if information does not allow to build a proper URI
     */
    public URI getURI() throws URISyntaxException {
        return new URI(scheme, null, host, port, null, null, null);
    }

}
