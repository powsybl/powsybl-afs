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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Utility class.
 *
 * @author Valentin Berthault {@literal <valentin.berthault at rte-france.com>}
 */
public final class Utils {

    private static final long MIN_DISK_SPACE_THRESHOLD = 10;
    public static final int DEFAULT_BUFFER = 512;
    public static final int DEFAULT_THRESHOLD_ENTRIES = 10000;
    public static final long DEFAULT_THRESHOLD_SIZE = 2000000000L; // 2 GB
    public static final double DEFAULT_THRESHOLD_RATIO = 20;

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    private Utils() throws IllegalAccessException {
        throw new IllegalAccessException();
        //not called
    }

    /**
     * zip a directory
     *
     * @param dir     directory path to zip
     * @param zipPath path to the zip to create
     * @throws IllegalArgumentException IllegalArgumentException
     */
    public static void zip(Path dir, Path zipPath, boolean deleteDirectory) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath));
            Stream<Path> walk = Files.walk(dir)) {
            walk.filter(someFileToZip -> !someFileToZip.equals(dir)).forEach(someFileToZip -> {
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
     *
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
        unzip(zipPath, nodeDir, DEFAULT_BUFFER, DEFAULT_THRESHOLD_ENTRIES, DEFAULT_THRESHOLD_SIZE, DEFAULT_THRESHOLD_RATIO);
    }

    /**
     * Unzip a file.
     *
     * @param zipPath          zip Path
     * @param nodeDir          path to the directory where unzip
     * @param thresholdEntries maximal number of entries
     * @param thresholdSize    maximal size of the zip file
     * @param thresholdRatio   maximal compression ratio
     */
    public static void unzip(Path zipPath, Path nodeDir, int buffer, int thresholdEntries, long thresholdSize, double thresholdRatio) throws IOException {
        // Normalize the file name
        Path path = zipPath.normalize();

        if (Files.notExists(path)) {
            throw new IOException("File does not exist: " + path.getFileName());
        }

        // Create the destination directory if needed
        if (Files.notExists(nodeDir)) {
            Files.createDirectories(nodeDir);
        }

        ZipEntry entry;
        int entries = 0;
        long total = 0;

        try (InputStream fis = Files.newInputStream(path);
             ZipInputStream zis = new ZipInputStream(new BufferedInputStream(fis))) {
            while ((entry = zis.getNextEntry()) != null) {
                // Compressed size
                long compressedEntrySize = entry.getCompressedSize();

                int count;
                long uncompressedEntrySize = 0;
                byte[] data = new byte[buffer];
                // Write the files to the disk, but ensure that the filename is valid,
                // and that the file is not insanely big
                String name = validateFilename(entry.getName(), nodeDir.toAbsolutePath().toString());
                Path entryPath = nodeDir.resolve(name);
                if (entry.isDirectory()) {
                    if (Files.notExists(entryPath)) {
                        Files.createDirectories(entryPath);
                    }
                    continue;
                }
                try (OutputStream fos = Files.newOutputStream(entryPath);
                     BufferedOutputStream dest = new BufferedOutputStream(fos, buffer)) {
                    while (total + buffer <= thresholdSize && (count = zis.read(data, 0, buffer)) != -1) {
                        dest.write(data, 0, count);
                        total += count;
                        uncompressedEntrySize += count;
                    }
                    dest.flush();
                    zis.closeEntry();
                    entries++;
                    if (entries > thresholdEntries) {
                        throw new IllegalStateException("Too many files to unzip.");
                    }
                    if (total + buffer > thresholdSize) {
                        throw new IllegalStateException("File being unzipped is too big.");
                    }
                    if ((double) uncompressedEntrySize / compressedEntrySize > thresholdRatio) {
                        throw new IllegalStateException("Suspicious compression ratio: " + (double) uncompressedEntrySize / compressedEntrySize);
                    }
                }

            }
        }
    }

    private static String validateFilename(String filename, String intendedDir) throws java.io.IOException {
        File f = new File(intendedDir, filename);
        String canonicalPath = f.getCanonicalPath();

        File iD = new File(intendedDir);
        String canonicalID = iD.getCanonicalPath();

        if (canonicalPath.startsWith(canonicalID)) {
            return canonicalPath;
        } else {
            throw new IllegalStateException("File is outside extraction target directory.");
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
