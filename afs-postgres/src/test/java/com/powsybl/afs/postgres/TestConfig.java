/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.powsybl.afs.storage.EventsBus;
import com.powsybl.afs.storage.InMemoryEventsBus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Configuration
@PropertySource("classpath:application.properties")
@Profile({"test"})
public class TestConfig {

    @Bean
    public EventsBus imMemory() {
        return new InMemoryEventsBus();
    }

    @Bean(name = "fileSystemName")
    public String getFileSystemName() {
        return "postgres-afs";
    }
}
