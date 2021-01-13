/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.powsybl.afs.storage.AbstractAppFileSystemConfig;
import com.powsybl.commons.config.ModuleConfig;
import com.powsybl.commons.config.PlatformConfig;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class PostgresAppFileSystemConfig extends AbstractAppFileSystemConfig<PostgresAppFileSystemConfig> {

    private final String ipAddress;
    private final String username;
    private final String password;

    public PostgresAppFileSystemConfig(String driveName, boolean remotelyAccessible, String ipAddress, String username, String password) {
        super(driveName, remotelyAccessible);
        this.ipAddress = Objects.requireNonNull(ipAddress);
        this.username = Objects.requireNonNull(username);
        this.password = Objects.requireNonNull(password);
    }

    public static List<PostgresAppFileSystemConfig> load() {
        return load(PlatformConfig.defaultConfig());
    }

    public static List<PostgresAppFileSystemConfig> load(PlatformConfig platformConfig) {
        ModuleConfig moduleConfig = platformConfig.getOptionalModuleConfig("postgres-app-file-system").orElse(null);
        if (moduleConfig != null) {
            final String driveName = moduleConfig.getStringProperty(DRIVE_NAME);
            final String ipAddress = moduleConfig.getStringProperty("ip-address");
            final String username = moduleConfig.getStringProperty("username");
            final String password = moduleConfig.getStringProperty("password");
            return Collections.singletonList(new PostgresAppFileSystemConfig(driveName, false, ipAddress, username, password));
        }
        return Collections.emptyList();
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
