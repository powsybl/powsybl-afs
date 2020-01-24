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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Utility class.
 *
 * @author Valentin Berthault <valentin.berthault at rte-france.com>
 */
public final class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
    private Utils() {
        //not called
    }

    /**
     * zip a directory
     *
     * @param dir directory path
     * @param zipPath path to the zip to create
     * @throws IllegalArgumentException IllegalArgumentException
     */
    public static void zip(Path dir, Path zipPath) throws IllegalArgumentException {
        try (FileOutputStream fos = new FileOutputStream(zipPath.toFile());
             ZipOutputStream zos = new ZipOutputStream(fos)) {
            Files.walk(dir)
                    .filter(someFileToZip -> !someFileToZip.equals(dir))
                    .forEach(
                        someFileToZip -> {
                            Path pathInZip = dir.relativize(someFileToZip);
                            if (Files.isDirectory(someFileToZip)) {
                                addDirectory(zos, pathInZip);
                            } else {
                                addFile(zos, someFileToZip, pathInZip);
                            }
                        });
        } catch (IOException e) {
            LOGGER.error("The file can't be added to the zip", e);
        }
        deleteDirectory(new File(dir.toString()));
    }

    /**
     * unzip
     *
     * @param zipPath zip Path
     * @param nodeDir path to the directory where unzip
     */
    public static void unzip(Path zipPath, Path nodeDir) {
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
        } catch (IOException e) {
            LOGGER.error("Unable to unzip {}", zipPath, e);
        }
    }

    private static void addDirectory(ZipOutputStream zos, Path relativeFilePath) {
        try {
            ZipEntry entry = new ZipEntry(relativeFilePath.toString() + "/");
            zos.putNextEntry(entry);
            zos.closeEntry();
        } catch (IOException e) {
            LOGGER.error("Unable to add directory {} to zip", relativeFilePath, e);
        }
    }

    private static void addFile(ZipOutputStream zos, Path filePath, Path zipFilePath) {
        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
            ZipEntry entry = new ZipEntry(zipFilePath.toString());
            zos.putNextEntry(entry);
            ByteStreams.copy(fis, zos);
        } catch (IOException e) {
            LOGGER.error("Unable to add file {} to zip", zipFilePath, e);
        }
    }

    /**
     * delete directory
     *
     * @param directoryToBeDeleted directory to be deleted
     * @return true if directory deleted
     */
    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[]allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
