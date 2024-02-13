/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.afs.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Valentin Berthault {@literal <valentin.berthault at rte-france.com>}
 */

class UtilsTest {

    private Path rootDir;

    @BeforeEach
    public void setup() throws IOException {
        rootDir = Files.createTempDirectory("test1");
        Path folder = rootDir.resolve("test");
        Files.createDirectory(folder);
        Files.createDirectory(folder.resolve("subTest"));
        Files.createFile(folder.resolve("test1"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        Utils.deleteDirectory(rootDir);
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
        Path rootDir2 = Files.createTempDirectory("test1");
        Files.createFile(rootDir2.resolve("test"));
        Utils.deleteDirectory(rootDir2);
        assertTrue(Files.notExists(rootDir2));
    }

    @Test
    void checkDiskSpaceTest() throws IOException {
        Path rootDir2 = Files.createTempDirectory("test1");
        Files.createFile(rootDir2.resolve("test"));
        try {
            Utils.checkDiskSpace(rootDir2);
        } catch (IOException ignored) {
        }

    }

}
