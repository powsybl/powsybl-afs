package com.powsybl.afs.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class UtilsTest {

    private Path rootDir;

    @Before
    public void setup() throws IOException {
        rootDir = Files.createTempDirectory("test1");
        Path folder = rootDir.resolve("test");
        Files.createDirectory(folder);
        Files.createDirectory(folder.resolve("subTest"));
        Files.createFile(folder.resolve("test1"));
    }

    @After
    public void tearDown() throws Exception {
        Utils.deleteDirectory(rootDir.toFile());
    }

    @Test
    public void zipAndUnzipTest() throws IOException {
        Path zipPath = rootDir.resolve("test.zip");
        Utils.zip(rootDir.resolve("test"), zipPath, true);

        Files.exists(zipPath);
        assertTrue(Files.exists(zipPath));

        Utils.unzip(zipPath, rootDir.resolve("test"));
        assertTrue(Files.exists(rootDir.resolve("test")));
    }

    @Test
    public void zipWithoutDeleteDirectoryTest() throws IOException {
        Path zipPath = rootDir.resolve("test.zip");
        Utils.zip(rootDir.resolve("test"), zipPath, false);
        Files.exists(zipPath);
        assertTrue(Files.exists(zipPath));
        assertTrue(Files.exists(rootDir.resolve("test")));
    }

    @Test
    public void deleteDirectoryTest() throws IOException {
        Path rootDir2 = Files.createTempDirectory("test1");
        Files.createFile(rootDir2.resolve("test"));
        Utils.deleteDirectory(rootDir2.toFile());
        assertTrue(Files.notExists(rootDir2));
    }

}
