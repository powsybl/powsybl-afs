/*
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Utility class.
 *
 * @author Valentin Berthault <valentin.berthault at rte-france.com>
 */
public final class Utils {

    private static final long MIN_DISK_SPACE_THRESHOLD = 10;

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    private Utils() throws IllegalAccessException {
        throw new IllegalAccessException();
        //not called
    }

    /**
     * zip a directory
     *
     * @param dir directory path to zip
     * @param zipPath path to the zip to create
     * @throws IllegalArgumentException IllegalArgumentException
     */
    public static void zip(Path dir, Path zipPath, boolean deleteDirectory) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(zipPath.toFile());
             ZipOutputStream zos = new ZipOutputStream(fos);
             Stream<Path> walk = Files.walk(dir);) {
            walk.filter(someFileToZip -> !someFileToZip.equals(dir))
                    .forEach(
                        someFileToZip -> {
                            Path pathInZip = dir.relativize(someFileToZip);
                            try {
                                if (Files.isDirectory(someFileToZip)) {
                                    addDirectory(zos, pathInZip);
                                } else {
                                    addFile(zos, someFileToZip, pathInZip);
                                }
                            } catch (IOException e) {
                                throw new AfsStorageException(e.getMessage());
                            }

                        });
        } catch (IOException | AfsStorageException e) {
            throw new IOException(e);
        }

        if (deleteDirectory) {
            deleteDirectory(new File(dir.toString()));
        }
    }

    /**
     * Check that there is enough space on the disk
     * @param dir directory to save
     * @throws IOException IOException
     */
    public static void checkDiskSpace(Path dir) throws IOException {
        File archiveFile = dir.toFile();
        long freeSpacePercent = 100 * archiveFile.getFreeSpace() / archiveFile.getTotalSpace();
        LOGGER.info("Copying into drive with {}% free space", freeSpacePercent);
        if (freeSpacePercent < MIN_DISK_SPACE_THRESHOLD) {
            throw new IOException("Not enough space");
        }
    }

    /**
     * unzip
     *
     * @param zipPath zip Path
     * @param nodeDir path to the directory where unzip
     */
    public static void unzip(Path zipPath, Path nodeDir) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipPath.toFile()))) {
            Files.createDirectories(nodeDir);
            ZipEntry entry = zis.getNextEntry();
            while (entry != null) {
                Path outputEntryPath = nodeDir.resolve(entry.getName());
                if (entry.isDirectory() && !Files.exists(outputEntryPath)) {
                    Files.createDirectory(outputEntryPath);
                } else if (!entry.isDirectory()) {
                    try (FileOutputStream fos = new FileOutputStream(outputEntryPath.toFile())) {
                        ByteStreams.copy(zis, fos);
                    }
                }
                entry = zis.getNextEntry();
            }
            zis.closeEntry();
        }
    }

    private static void addDirectory(ZipOutputStream zos, Path relativeFilePath) throws IOException {
        ZipEntry entry = new ZipEntry(relativeFilePath.toString() + "/");
        zos.putNextEntry(entry);
        zos.closeEntry();
    }

    private static void addFile(ZipOutputStream zos, Path filePath, Path zipFilePath) throws IOException {
        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
            ZipEntry entry = new ZipEntry(zipFilePath.toString());
            zos.putNextEntry(entry);
            ByteStreams.copy(fis, zos);
        }
    }

    /**
     * delete directory
     *
     * @param directoryToBeDeleted directory to be deleted
     * @return true if directory deleted
     */
    public static void deleteDirectory(File directoryToBeDeleted) throws IOException {
        File[]allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        Files.delete(directoryToBeDeleted.toPath());
    }
}
