/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.local.storage;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.ext.base.Case;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.iidm.network.Importer;
import com.powsybl.iidm.serde.XMLImporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

import static com.powsybl.afs.local.storage.LocalCase.METHOD_NOT_IMPLEMENTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class LocalCaseTest {
    private final String fileName = "test.xiidm";
    private final Importer importer = new XMLImporter();
    private FileSystem fileSystem;
    private Path testDir;
    private LocalCase localCase;

    @BeforeEach
    void setUp() throws Exception {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        testDir = fileSystem.getPath("/tmp");
        Files.createDirectories(testDir);
        Path testFile = testDir.resolve(fileName);
        Files.createFile(testFile);
        localCase = new LocalCase(testFile, importer);
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.close();
    }

    @Test
    void getNameTest() {
        assertEquals("test", localCase.getName());
    }

    @Test
    void getParentPathTest() {
        assertEquals(Optional.of(testDir), localCase.getParentPath());
    }

    @Test
    void getPseudoClassTest() {
        assertEquals(Case.PSEUDO_CLASS, localCase.getPseudoClass());
    }

    @Test
    void getDescriptionTest() {
        assertEquals(importer.getComment(), localCase.getDescription());
    }

    @Test
    void getGenericMetadataTest() {
        assertEquals(new NodeGenericMetadata().setString("format", importer.getFormat()), localCase.getGenericMetadata());
    }

    @Test
    void dataExistsTest() {
        // Incorrect name
        assertThrows(AfsException.class, () -> localCase.dataExists("test"));

        // FileName pattern
        assertFalse(localCase.dataExists("DATA_SOURCE_FILE_NAME__test"));

        // SuffixAndExtension pattern
        assertFalse(localCase.dataExists("DATA_SOURCE_SUFFIX_EXT__test__ext"));
    }

    @Test
    void getDataNamesTest() {
        Set<String> names = localCase.getDataNames();
        assertEquals(1, names.size());
        assertEquals(fileName, names.iterator().next());
    }

    @Test
    void unimplementedMethodsTest() {
        AfsException exception;
        exception = assertThrows(AfsException.class, () -> localCase.getTimeSeriesNames());
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
        exception = assertThrows(AfsException.class, () -> localCase.timeSeriesExists("any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
        exception = assertThrows(AfsException.class, () -> localCase.getTimeSeriesDataVersions());
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
        exception = assertThrows(AfsException.class, () -> localCase.getTimeSeriesDataVersions("any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
        Set<String> nameSet = Set.of("any");
        exception = assertThrows(AfsException.class, () -> localCase.getTimeSeriesMetadata(nameSet));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
        exception = assertThrows(AfsException.class, () -> localCase.getDoubleTimeSeriesData(nameSet, 0));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
        exception = assertThrows(AfsException.class, () -> localCase.getStringTimeSeriesData(nameSet, 0));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }
}
