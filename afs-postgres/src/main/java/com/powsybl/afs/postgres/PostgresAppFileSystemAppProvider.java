/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.google.auto.service.AutoService;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProvider;
import com.powsybl.afs.AppFileSystemProviderContext;
import com.powsybl.afs.storage.EventsBus;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@AutoService(AppFileSystemProvider.class)
public class PostgresAppFileSystemAppProvider implements AppFileSystemProvider {

    private final List<PostgresAppFileSystemConfig> configs;

    public PostgresAppFileSystemAppProvider() {
        this(PostgresAppFileSystemConfig.load());
    }

    public PostgresAppFileSystemAppProvider(List<PostgresAppFileSystemConfig> configs) {
        this.configs = Objects.requireNonNull(configs);
    }

    @Override
    public List<AppFileSystem> getFileSystems(AppFileSystemProviderContext context) {
        final PostgresAppFileSystemConfig postgresAppFileSystemConfig = configs.get(0);
        AnnotationConfigApplicationContext springContext
                = new AnnotationConfigApplicationContext();
        springContext.getEnvironment().setActiveProfiles("production");
        springContext.scan("com.powsybl.afs.postgres");
        springContext.register(PostgresAppStorage.class);
        springContext.getBeanFactory().registerSingleton(EventsBus.class.toString(), context.getEventsBus());
        springContext.getBeanFactory().registerSingleton(String.class.toString(), postgresAppFileSystemConfig.getDriveName());
        springContext.refresh();
        final PostgresAppStorage bean = springContext.getBean(PostgresAppStorage.class);
        return Collections.singletonList(new PostgresAppFileSystem(postgresAppFileSystemConfig.getDriveName(), true, bean));
    }
}
