/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.google.common.io.ByteStreams;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.AppStorageArchive;
import com.powsybl.afs.storage.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Base class for all node objects stored in an AFS tree.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public abstract class AbstractNodeBase<F> {

    protected final NodeInfo info;

    protected final AppStorage storage;

    protected int codeVersion;

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNodeBase.class);

    public AbstractNodeBase(NodeInfo info, AppStorage storage, int codeVersion) {
        this.info = Objects.requireNonNull(info);
        this.storage = Objects.requireNonNull(storage);
        this.codeVersion = codeVersion;
    }

    public abstract Optional<F> getParent();

    protected Optional<NodeInfo> getParentInfo() {
        return storage.getParentNode(info.getId());
    }

    /**
     * An ID uniquely identifying this node in the file system tree.
     */
    public String getId() {
        return info.getId();
    }

    public String getName() {
        return info.getName();
    }

    public String getDescription() {
        return info.getDescription();
    }

    public void setDescription(String description) {
        storage.setDescription(info.getId(), description);
        info.setDescription(description);
        storage.flush();
    }

    public ZonedDateTime getCreationDate() {
        return Instant.ofEpochMilli(info.getCreationTime()).atZone(ZoneId.systemDefault());
    }

    public ZonedDateTime getModificationDate() {
        return Instant.ofEpochMilli(info.getModificationTime()).atZone(ZoneId.systemDefault());
    }

    public int getVersion() {
        return info.getVersion();
    }

    protected int getCodeVersion() {
        return codeVersion;
    }

    public boolean isAheadOfVersion() {
        return info.getVersion() > getCodeVersion();
    }

    public abstract NodePath getPath();

    public abstract boolean isFolder();

    private NodeInfo getChildInfo(NodeInfo nodeInfo, String name) {
        Objects.requireNonNull(name);
        NodeInfo childInfo = nodeInfo;
        for (String name2 : name.split(AppFileSystem.PATH_SEPARATOR)) {
            childInfo = storage.getChildNode(childInfo.getId(), name2).orElse(null);
            if (childInfo == null) {
                return null;
            }
        }
        return childInfo;
    }

    protected NodeInfo getChildInfo(String name, String... more) {
        NodeInfo childInfo = getChildInfo(info, name);
        if (childInfo == null) {
            return null;
        }
        for (String name2 : more) {
            childInfo = getChildInfo(childInfo, name2);
            if (childInfo == null) {
                return null;
            }
        }
        return childInfo;
    }

    @Override
    public String toString() {
        return getName();
    }

    public void moveTo(AbstractNodeBase<F> folder) {
        Objects.requireNonNull(folder);
        if (!folder.isParentOf(this)) {
            if (isMovableTo(folder)) {
                storage.setParentNode(info.getId(), folder.getId());
                storage.flush();
            } else {
                throw new AfsException("The source node is an ancestor of the target node");
            }
        }
    }

    private boolean isMovableTo(AbstractNodeBase<F> node) {
        return node.isFolder() && !isAncestorOf(node);
    }

    public boolean isAncestorOf(AbstractNodeBase<F> node) {
        Optional<NodeInfo> current = storage.getParentNode(node.getId());
        while (current.isPresent()) {
            if (current.get().getId().equals(info.getId())) {
                return true;
            } else {
                current = storage.getParentNode(current.get().getId());
            }
        }
        return false;
    }

    boolean isParentOf(AbstractNodeBase<F> node) {
        return node.getParentInfo().map(n -> n.getId().equals(info.getId())).orElse(false);
    }

    public void rename(String name) {
        Objects.requireNonNull(name);
        if (!nodeNameAlreadyExists(name)) {
            storage.renameNode(info.getId(), name);
            info.setName(name);
            storage.flush();
        } else {
            throw new AfsException("name already exists");
        }
    }

    private boolean nodeNameAlreadyExists(String name) {
        Objects.requireNonNull(name);
        Optional<NodeInfo> parentNode = storage.getParentNode(getId());
        List<NodeInfo> childNodes = new ArrayList<>();
        if (parentNode.isPresent()) {
            childNodes = storage.getChildNodes(parentNode.get().getId());
        }
        return childNodes.stream().filter(nodeInfo -> !nodeInfo.getId().equals(getId())).anyMatch(nodeInfo -> nodeInfo.getName().equals(name));
    }

    public void archive(Path dir){
        Objects.requireNonNull(dir);
        try {
            new AppStorageArchive(storage).archive(info, dir);
            zipDirectory(dir, info);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void zipDirectory(Path nodeDir, NodeInfo info) throws IOException,
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

    private void addDirectory(ZipOutputStream zos, Path relativeFilePath) {
        try {
            ZipEntry entry = new ZipEntry(relativeFilePath.toString() + "/");
            zos.putNextEntry(entry);
            zos.closeEntry();
        } catch (IOException e) {
            LOGGER.error("Unable to add directory {} to zip", relativeFilePath, e);
        }
    }

    private void addFile(ZipOutputStream zos, Path filePath, Path zipFilePath) {
        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
            ZipEntry entry = new ZipEntry(zipFilePath.toString());
            zos.putNextEntry(entry);
            ByteStreams.copy(fis, zos);
        } catch (IOException e) {
            LOGGER.error("Unable to add file {} to zip", zipFilePath, e);
        }
    }

    boolean deleteDirectory(File directoryToBeDeleted) {
        File[]allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public void unarchive(Path dir) {
        new AppStorageArchive(storage).unarchive(info, dir);
    }
}
