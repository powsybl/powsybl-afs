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

public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static void zip(Path nodeDir, NodeInfo info) throws IOException,
            IllegalArgumentException{
        Path zipPath = nodeDir.resolve(info.getId() + ".zip");
        try (FileOutputStream fos = new FileOutputStream(zipPath.toFile());
             ZipOutputStream zos = new ZipOutputStream(fos)) {
            Files.walk(nodeDir.resolve(info.getId()))
                    .filter(someFileToZip -> !someFileToZip.equals(nodeDir.resolve(info.getId())))
                    .forEach(
                            someFileToZip -> {
                                Path pathInZip = nodeDir.resolve(info.getId()).relativize(someFileToZip);
                                if (Files.isDirectory(someFileToZip)) {
                                    addDirectory(zos, pathInZip);
                                } else {
                                    addFile(zos, someFileToZip, pathInZip);
                                }
                            });
        } catch (IOException e) {
            LOGGER.error("The file can't be added to the zip", e);
        }
        deleteDirectory(new File(nodeDir.resolve(info.getId()).toString()));
    }

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
            LOGGER.error("Unable to unzip", zipPath, e);
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
