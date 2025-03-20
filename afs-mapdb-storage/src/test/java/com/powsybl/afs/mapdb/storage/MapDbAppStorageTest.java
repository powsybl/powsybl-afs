/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mapdb.storage;

import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class MapDbAppStorageTest extends AbstractAppStorageTest {

    private static final String STRING_MEM = "mem";

    @Override
    protected AppStorage createStorage() {
        return createStorage(STRING_MEM);
    }

    @Override
    protected AppStorage createStorage(String fileSystemName) {
        return MapDbAppStorage.createMem(fileSystemName, new InMemoryEventsBus());
    }

    @Test
    void createMemTest() {
        InMemoryEventsBus eventsBus = new InMemoryEventsBus();
        MapDbAppStorage mapDbAppStorage = MapDbAppStorage.createMem(STRING_MEM, eventsBus);

        assertEquals(eventsBus, mapDbAppStorage.getEventsBus());
        assertEquals(STRING_MEM, mapDbAppStorage.getFileSystemName());
    }

    @Test
    void createHeapTest() {
        InMemoryEventsBus eventsBus = new InMemoryEventsBus();
        MapDbAppStorage mapDbAppStorage = MapDbAppStorage.createHeap(STRING_MEM, eventsBus);

        assertEquals(eventsBus, mapDbAppStorage.getEventsBus());
        assertEquals(STRING_MEM, mapDbAppStorage.getFileSystemName());
    }

    @Test
    void createMmapFileTest() throws IOException {
        // Create a temporary file
        File dbFile = File.createTempFile("mapdb", ".db");

        // Delete the file so that MapDB can create a valid one
        assertTrue(dbFile.delete());

        // Create the MapDbAppStorage
        InMemoryEventsBus eventsBus = new InMemoryEventsBus();
        MapDbAppStorage mapDbAppStorage = MapDbAppStorage.createMmapFile(STRING_MEM, dbFile, eventsBus);

        assertEquals(eventsBus, mapDbAppStorage.getEventsBus());
        assertEquals(STRING_MEM, mapDbAppStorage.getFileSystemName());

        // Clean the file
        dbFile.deleteOnExit();
    }
}
