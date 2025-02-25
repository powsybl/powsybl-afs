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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Utility class.
 *
 * @author Valentin Berthault {@literal <valentin.berthault at rte-france.com>}
 */
public final class Utils {

    private static final long MIN_DISK_SPACE_THRESHOLD = 10;
    public static final int THRESHOLD_ENTRIES = 10000;
    public static final long THRESHOLD_SIZE = 1000000000L; // 1 GB
    public static final double THRESHOLD_RATIO = 10;

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
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath));
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
            deleteDirectory(dir);
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
        unzip(zipPath, nodeDir, THRESHOLD_ENTRIES, THRESHOLD_SIZE, THRESHOLD_RATIO);
    }

    /**
     * Unzip a file. <i>Currently does not work with Jimfs generated files</i>
     *
     * @param zipPath zip Path
     * @param nodeDir path to the directory where unzip
     * @param thresholdEntries maximal number of entries
     * @param thresholdSize maximal size of the zip file
     * @param thresholdRatio maximal compression ratio
     */
    public static void unzip(Path zipPath, Path nodeDir, int thresholdEntries, long thresholdSize, double thresholdRatio) throws IOException {
        // Normalize the file name
        Path path = zipPath.normalize();

        if (Files.notExists(path)) {
            throw new IOException("File does not exist: " + zipPath.normalize().getFileName());
        }

        try (ZipFile zipFile = new ZipFile(String.valueOf(path))) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();

            AtomicInteger totalSizeArchive = new AtomicInteger(0);
            int totalEntryArchive = 0;

            while (entries.hasMoreElements()) {
                ZipEntry ze = entries.nextElement();
                Path outputEntryPath = nodeDir.resolve(ze.getName()).normalize();
                totalEntryArchive++;

                // Validation against Zip Slip
                if (!outputEntryPath.startsWith(nodeDir)) {
                    throw new IOException("Invalid path detected: " + ze.getName());
                }

                if (ze.isDirectory()) {
                    Files.createDirectories(outputEntryPath);
                } else {
                    try (InputStream in = new BufferedInputStream(zipFile.getInputStream(ze));
                        OutputStream out = new BufferedOutputStream(new FileOutputStream(String.valueOf(outputEntryPath)))) {

                        copyZipEntry(ze, in, out, totalSizeArchive, thresholdSize, thresholdRatio);
                    }
                }

                if (totalEntryArchive > thresholdEntries) {
                    // too many entries in this archive, can lead to inodes exhaustion of the system
                    throw new IOException("Too many entries in this archive: " + zipPath.normalize().getFileName());
                }
            }
        }
    }

    private static void copyZipEntry(ZipEntry ze, InputStream in, OutputStream out, AtomicInteger totalSizeArchive, long thresholdSize, double thresholdRatio) throws IOException {
        int nBytes;
        byte[] buffer = new byte[2048];
        int totalSizeEntry = 0;

        while ((nBytes = in.read(buffer)) > 0) {
            out.write(buffer, 0, nBytes);
            totalSizeEntry += nBytes;
            totalSizeArchive.getAndAdd(nBytes);

            double compressionRatio = (double) totalSizeEntry / ze.getCompressedSize();
            if (compressionRatio > thresholdRatio) {
                // ratio between compressed and uncompressed data is highly suspicious, looks like a Zip Bomb Attack
                throw new IOException("Ratio between compressed and uncompressed data is highly suspicious in: " + ze.getName());
            }
        }

        if (totalSizeArchive.get() > thresholdSize) {
            // the uncompressed data size is too much for the application resource capacity
            throw new IOException("Uncompressed data size is too much for the application resource capacity in: " + ze.getName());
        }
    }

    private static void addDirectory(ZipOutputStream zos, Path relativeFilePath) throws IOException {
        ZipEntry entry = new ZipEntry(relativeFilePath.toString() + "/");
        zos.putNextEntry(entry);
        zos.closeEntry();
    }

    private static void addFile(ZipOutputStream zos, Path filePath, Path zipFilePath) throws IOException {
        try (InputStream fis = Files.newInputStream(filePath)) {
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
    public static void deleteDirectory(Path directoryToBeDeleted) throws IOException {
        if (Files.isDirectory(directoryToBeDeleted)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(directoryToBeDeleted)) {
                for (Path entry : entries) {
                    deleteDirectory(entry);
                }
            }
        }
        Files.delete(directoryToBeDeleted);
    }
}
