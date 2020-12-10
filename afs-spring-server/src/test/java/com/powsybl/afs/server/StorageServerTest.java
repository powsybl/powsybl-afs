/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.afs.server;

import com.google.common.collect.ImmutableList;
import com.powsybl.afs.AppData;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProvider;
import com.powsybl.afs.LocalTaskMonitor;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.EventsBus;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.ws.storage.RemoteAppStorage;
import com.powsybl.commons.exceptions.UncheckedUriSyntaxException;
import com.powsybl.computation.ComputationManager;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.servlet.ServletContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = StorageServer.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EnableAutoConfiguration
@ActiveProfiles("test")
public class StorageServerTest extends AbstractAppStorageTest {

    @LocalServerPort
    private int port;

    @Autowired
    private ServletContext servletContext;

    @TestConfiguration
    public static class AppDataConfig {
        @Bean
        public AppData getAppData() {
            EventsBus eventBus = new InMemoryEventsBus();
            AppStorage storage = MapDbAppStorage.createMem("mem", eventBus);
            AppFileSystem fs = new AppFileSystem("test", true, storage, new LocalTaskMonitor());
            ComputationManager cm = Mockito.mock(ComputationManager.class);

            List<AppFileSystemProvider> fsProviders = ImmutableList.of(m -> ImmutableList.of(fs));
            return new AppData(cm, cm, fsProviders, eventBus);
        }
    }

    private URI getRestUri() {
        try {
            String sheme = "http";
            return new URI(sheme + "://localhost:" + port + servletContext.getContextPath());
        } catch (URISyntaxException e) {
            throw new UncheckedUriSyntaxException(e);
        }
    }

    @Override
    protected AppStorage createStorage() {
        URI restUri = getRestUri();
        return new RemoteAppStorage("test", restUri, "");
    }
}
