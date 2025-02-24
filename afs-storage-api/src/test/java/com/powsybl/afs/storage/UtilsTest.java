/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.afs.storage;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Valentin Berthault {@literal <valentin.berthault at rte-france.com>}
 */

class UtilsTest {
    private FileSystem fileSystem;
    private Path rootDir;

    @BeforeEach
    void setup() throws IOException {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        rootDir = Files.createTempDirectory(String.valueOf(fileSystem.getPath("test1")));
        Path folder = rootDir.resolve("test");
        Files.createDirectory(folder);
        Files.createDirectory(folder.resolve("subTest"));
        Files.createFile(folder.resolve("test1"));
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.close();
    }

    @Test
    void zipAndUnzipTest() throws IOException {
        Path zipPath = rootDir.resolve("test.zip");
        Utils.zip(rootDir.resolve("test"), zipPath, true);

        Files.exists(zipPath);
        assertTrue(Files.exists(zipPath));

        Utils.unzip(zipPath, rootDir.resolve("test"));
        assertTrue(Files.exists(rootDir.resolve("test")));
    }

    @Test
    void zipWithoutDeleteDirectoryTest() throws IOException {
        Path zipPath = rootDir.resolve("test.zip");
        Utils.zip(rootDir.resolve("test"), zipPath, false);
        Files.exists(zipPath);
        assertTrue(Files.exists(zipPath));
        assertTrue(Files.exists(rootDir.resolve("test")));
    }

    @Test
    void deleteDirectoryTest() throws IOException {
        Files.createFile(rootDir.resolve("test2"));
        Utils.deleteDirectory(rootDir);
        assertTrue(Files.notExists(rootDir));
    }

    @Test
    void checkDiskSpaceTest() throws IOException {
        Files.createFile(rootDir.resolve("test3"));
        Utils.checkDiskSpace(rootDir);
    }

}
