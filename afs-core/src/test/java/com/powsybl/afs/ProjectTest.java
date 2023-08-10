/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.afs;

import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

/**
 * @author Amira KAHYA <amira.kahya at rte-france.com>
 */
class ProjectTest {
    private static final String FOLDER_PSEUDO_CLASS = "folder";

    private AppStorage storage;
    private AppFileSystem afs;

    @BeforeEach
    void setup() {
        storage = MapDbAppStorage.createMem("mem", new InMemoryEventsBus());

        ComputationManager computationManager = Mockito.mock(ComputationManager.class);
        afs = new AppFileSystem("mem", true, storage);
    }

    @Test
    void createProjectFolderTest() throws IOException {
        Project project = afs.getRootFolder().createProject("test");
        NodeInfo info = storage.createNode(project.getId(), "test", FOLDER_PSEUDO_CLASS, "d", 0,
                new NodeGenericMetadata().setString("k", "v"));
        ProjectFolder projectFolder = project.createProjectFolder(info);
        assertNotNull(projectFolder);
        assertEquals("d", projectFolder.getDescription());
        assertEquals(0, projectFolder.getVersion());
        assertEquals("test", projectFolder.getName());
        assertTrue(storage.getNodeInfo(projectFolder.getId()).getCreationTime() > 0);
        assertTrue(storage.getNodeInfo(projectFolder.getId()).getModificationTime() > 0);
    }

    @AfterEach
    public void tearDown() {
        storage.close();
    }
}
