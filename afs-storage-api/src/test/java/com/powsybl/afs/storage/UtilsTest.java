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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void unzipNotVulnerableToZipSlipTest() throws IOException {
        // Given
        // A zip file with entry with a name containing ../
        Path zipPath = rootDir.resolve("file.zip");
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(Files.newOutputStream(zipPath))) {
            // A malicious entry which name contains a parent directory reference
            ZipEntry e = new ZipEntry("../malicious.txt");
            zipOutputStream.putNextEntry(e);
            zipOutputStream.write("Hello World !".getBytes());
        }
        // Given an output directory
        Path outputDir = rootDir.resolve("output_dir");
        // Unzipping the entry should throw an exception
        assertThatThrownBy(() -> Utils.unzip(zipPath, outputDir))
                .isInstanceOf(IOException.class)
                .hasMessage("Archive entry '../malicious.txt' would extract outside of the working directory");
        // The entry should not be unzipped
        assertThat(outputDir.resolve("../malicious.txt")).doesNotExist();
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
