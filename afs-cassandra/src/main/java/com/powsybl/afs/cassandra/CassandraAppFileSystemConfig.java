/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.powsybl.afs.storage.AbstractAppFileSystemConfig;
import com.powsybl.commons.config.ModuleConfig;
import com.powsybl.commons.config.PlatformConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class CassandraAppFileSystemConfig extends AbstractAppFileSystemConfig<CassandraAppFileSystemConfig> {

    private List<String> ipAddresses;

    private String localDc;
    private static final String DRIVE_NAME = "drive-name";
    private static final String IP_ADRESSES = "ip-addresses";
    private static final String REMOTELY_ACCESSIBLE = "remotely-accessible";
    private static final String LOCAL_DC = "local-dc";

    public static List<CassandraAppFileSystemConfig> load() {
        return load(PlatformConfig.defaultConfig());
    }

    public static List<CassandraAppFileSystemConfig> load(PlatformConfig platformConfig) {
        List<CassandraAppFileSystemConfig> configs = new ArrayList<>();
        ModuleConfig moduleConfig = platformConfig.getOptionalModuleConfig("cassandra-app-file-system").orElse(null);
        if (moduleConfig != null) {
            if (moduleConfig.hasProperty(DRIVE_NAME)
                    && moduleConfig.hasProperty(IP_ADRESSES)) {
                String driveName = moduleConfig.getStringProperty(DRIVE_NAME);
                boolean remotelyAccessible = moduleConfig.getBooleanProperty(REMOTELY_ACCESSIBLE, DEFAULT_REMOTELY_ACCESSIBLE);
                List<String> ipAddresses = moduleConfig.getStringListProperty(IP_ADRESSES);
                String localDc = moduleConfig.getStringProperty(LOCAL_DC, null);
                configs.add(new CassandraAppFileSystemConfig(driveName, remotelyAccessible, ipAddresses, localDc));
            }
            int maxAdditionalDriveCount = moduleConfig.getIntProperty("max-additional-drive-count", 0);
            for (int i = 0; i < maxAdditionalDriveCount; i++) {
                if (moduleConfig.hasProperty(DRIVE_NAME + "-" + i)
                        && moduleConfig.hasProperty(IP_ADRESSES + "-" + i)) {
                    String driveName = moduleConfig.getStringProperty(DRIVE_NAME + "-" + i);
                    boolean remotelyAccessible = moduleConfig.getBooleanProperty(REMOTELY_ACCESSIBLE + "-" + i, DEFAULT_REMOTELY_ACCESSIBLE);
                    List<String> ipAddresses = moduleConfig.getStringListProperty(IP_ADRESSES + "-" + i);
                    String localDc = moduleConfig.getStringProperty(LOCAL_DC + "-" + i, null);
                    configs.add(new CassandraAppFileSystemConfig(driveName, remotelyAccessible, ipAddresses, localDc));
                }
            }
        }
        return configs;
    }

    public CassandraAppFileSystemConfig(String driveName, boolean remotelyAccessible, List<String> ipAddresses, String localDc) {
        super(driveName, remotelyAccessible);
        this.ipAddresses = checkIpAddresses(ipAddresses);
        this.localDc = localDc;
    }

    private static List<String> checkIpAddresses(List<String> ipAddresses) {
        Objects.requireNonNull(ipAddresses);
        if (ipAddresses.isEmpty()) {
            throw new IllegalArgumentException("Empty IP address list");
        }
        return ipAddresses;
    }

    public List<String> getIpAddresses() {
        return Collections.unmodifiableList(ipAddresses);
    }

    public CassandraAppFileSystemConfig setIpAddress(List<String> ipAddresses) {
        this.ipAddresses = new ArrayList<>(checkIpAddresses(ipAddresses));
        return this;
    }

    public String getLocalDc() {
        return localDc;
    }

    public CassandraAppFileSystemConfig setLocalDc(String localDc) {
        this.localDc = localDc;
        return this;
    }
}
