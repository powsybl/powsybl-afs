/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.local.storage;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.AfsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DefaultLocalFolderTest {

    private FileSystem fileSystem;

    private Path rootDir;

    private Path subDir;

    @BeforeEach
    void setUp() throws Exception {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        rootDir = fileSystem.getPath("/cases");
        subDir = rootDir.resolve("sub");
        Files.createDirectories(subDir);
        Files.createFile(rootDir.resolve("n.tst"));
        Files.createFile(subDir.resolve("s.tst"));

        Path outsideDir = fileSystem.getPath("/outside");
        Files.createDirectories(outsideDir);
        Files.createFile(outsideDir.resolve("o.tst"));

        Path siblingDir = fileSystem.getPath("/cases2");
        Files.createDirectories(siblingDir);
        Files.createFile(siblingDir.resolve("c.tst"));
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"/outside", "/cases/../outside", "/"})
    void constructorRejectsDirOutsideRootDirTest(String dirPath) {
        Path dir = fileSystem.getPath(dirPath);
        AfsException exception = assertThrows(AfsException.class, () -> new DefaultLocalFolder(dir, rootDir, "mem"));
        assertEquals("directory '" + dirPath + "' is not located inside the root directory '/cases'", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"/cases", "/cases/../cases", "/cases/sub", "/cases/./sub", "/cases/sub/deeper"})
    void constructorAcceptsDirInsideRootDirTest(String dirPath) {
        assertDoesNotThrow(() -> new DefaultLocalFolder(fileSystem.getPath(dirPath), rootDir, "mem"));
    }

    @Test
    void getChildPathsTest() {
        // case dir = root directory
        DefaultLocalFolder root = new DefaultLocalFolder(rootDir, rootDir, "mem");
        List<Path> childPaths = root.getChildPaths();
        assertEquals(2, childPaths.size());
        assertTrue(childPaths.containsAll(List.of(rootDir.resolve("n.tst"), subDir)));

        // case dir = sub directory
        DefaultLocalFolder sub = new DefaultLocalFolder(subDir, rootDir, "mem");
        assertEquals(List.of(subDir.resolve("s.tst")), sub.getChildPaths());
    }

    @Test
    void getChildPathTest() {
        DefaultLocalFolder root = new DefaultLocalFolder(rootDir, rootDir, "mem");
        assertEquals(Optional.of(rootDir.resolve("n.tst")), root.getChildPath("n.tst"));
        // Nested child
        assertEquals(Optional.of(subDir.resolve("s.tst")), root.getChildPath("sub/s.tst"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"./n.tst", "sub/../n.tst", "../cases/n.tst", "/cases/n.tst", "/cases/../cases/n.tst"})
    void getChildPathStayingInsideDirTest(String name) {
        DefaultLocalFolder root = new DefaultLocalFolder(rootDir, rootDir, "mem");
        Path childPath = root.getChildPath(name).orElseThrow();
        assertEquals(rootDir.resolve("n.tst"), childPath);
    }

    @ParameterizedTest
    @ValueSource(strings = {"../../n.tst", "..", "../../outside", "../../outside/o.tst", "/cases/n.tst", "/outside", "/outside/o.tst",
        "/", "/cases", "..", ".", ""})
    void getChildPathTraversalIsRejectedTest(String name) {
        DefaultLocalFolder sub = new DefaultLocalFolder(subDir, rootDir, "mem");
        AfsException exception = assertThrows(AfsException.class, () -> sub.getChildPath(name));
        assertEquals("Child name '" + name + "' is not located inside the directory '/cases/sub'", exception.getMessage());
    }

    @Test
    void getNameTest() {
        assertEquals("mem", new DefaultLocalFolder(rootDir, rootDir, "mem").getName());
        assertEquals("mem", new DefaultLocalFolder(fileSystem.getPath("/cases/sub/.."), rootDir, "mem").getName());
        assertEquals("sub", new DefaultLocalFolder(subDir, rootDir, "mem").getName());
        assertEquals("deeper", new DefaultLocalFolder(subDir.resolve("deeper"), rootDir, "mem").getName());
    }

    @Test
    void getParentPathTest() {
        // Cases with parent
        assertEquals(Optional.of(rootDir), new DefaultLocalFolder(subDir, rootDir, "mem").getParentPath());
        assertEquals(Optional.of(subDir), new DefaultLocalFolder(subDir.resolve("deeper"), rootDir, "mem").getParentPath());

        // Cases with no parent
        assertTrue(new DefaultLocalFolder(rootDir, rootDir, "mem").getParentPath().isEmpty());
        assertTrue(new DefaultLocalFolder(fileSystem.getPath("/cases/sub/.."), rootDir, "mem").getParentPath().isEmpty());
    }
}
