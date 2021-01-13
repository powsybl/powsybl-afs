/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.config;

import com.powsybl.afs.postgres.PostgresAppFileSystemConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.List;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Configuration
public class JpaContext {

    @Bean
    public DataSource dataSource() {
        final List<PostgresAppFileSystemConfig> load = PostgresAppFileSystemConfig.load();
        final PostgresAppFileSystemConfig config = load.get(0);
        if (config != null) {
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName("org.postgresql.Driver");
            dataSource.setUsername(config.getUsername());
            dataSource.setPassword(config.getPassword());
            dataSource.setUrl(
                    "jdbc:postgresql://" + config.getIpAddress() + "/" + config.getDriveName());
            return dataSource;
        }
        return null;
    }
}
