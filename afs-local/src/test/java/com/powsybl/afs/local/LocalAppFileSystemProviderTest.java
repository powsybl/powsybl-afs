/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.local;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProviderContext;
import com.powsybl.afs.local.storage.LocalCaseScanner;
import com.powsybl.afs.local.storage.LocalFileScanner;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.computation.ComputationManager;
import com.powsybl.iidm.network.ImportConfig;
import com.powsybl.iidm.network.ImportersLoaderList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.FileSystem;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class LocalAppFileSystemProviderTest {

    private FileSystem fileSystem;

    @BeforeEach
    public void setUp() throws Exception {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
    }

    @AfterEach
    public void tearDown() throws Exception {
        fileSystem.close();
    }

    @Test
    void test() {
        ComputationManager computationManager = Mockito.mock(ComputationManager.class);
        LocalAppFileSystemConfig config = new LocalAppFileSystemConfig("drive", true, fileSystem.getPath("/work"));
        LocalFileScanner extension = new LocalCaseScanner(new ImportConfig(), new ImportersLoaderList());
        List<AppFileSystem> fileSystems = new LocalAppFileSystemProvider(Collections.singletonList(config),
                                                                         Collections.singletonList(extension),
                                                                         Collections.emptyList())
                .getFileSystems(new AppFileSystemProviderContext(computationManager, null, new InMemoryEventsBus()));
        assertEquals(1, fileSystems.size());
        assertInstanceOf(LocalAppFileSystem.class, fileSystems.get(0));
        assertEquals("drive", fileSystems.get(0).getName());
        assertTrue(fileSystems.get(0).isRemotelyAccessible());
    }
}
