/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.EventsBus;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An AppFileSystem instance is a tree of {@link Node} objects, starting with its root folder.
 * <p>
 * {@link Node} objects may be {@link Folder}s or {@link File}, or any new file type added
 * by the user through the extension mechanism (see {@link FileExtension}).
 * An application may have several instances of {@link AppFileSystem}, each one with a unique name.
 * They are accessed through the parent {@link AppData} instance.
 *
 * <p>
 * The AppFileSystem is backed by an {@link AppStorage} implementation, which is in charge of maintaining
 * the state of data of the AppFileSystem. The implementation may bring additional functionalities:
 * in-memory storage, database storage, remote storage, ...
 *
 * <p>
 * Users of an AppFileSystem should not need to interact directly with the underlying {@link AppStorage}.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class AppFileSystem implements AutoCloseable {

    public static final String FS_SEPARATOR = ":";
    public static final String PATH_SEPARATOR = "/";

    private final String name;

    private final boolean remotelyAccessible;

    private final AppStorage storage;

    private final Supplier<NodeInfo> rootNodeInfo;

    private final TaskMonitor taskMonitor;

    private AppData data;

    public AppFileSystem(String name, boolean remotelyAccessible, AppStorage storage) {
        this(name, remotelyAccessible, storage, new LocalTaskMonitor());
    }

    public AppFileSystem(String name, boolean remotelyAccessible, AppStorage storage, TaskMonitor taskMonitor) {
        this.name = Objects.requireNonNull(name);
        this.remotelyAccessible = remotelyAccessible;
        this.storage = Objects.requireNonNull(storage);
        this.taskMonitor = Objects.requireNonNull(taskMonitor);
        rootNodeInfo = Suppliers.memoize(() -> storage.createRootNodeIfNotExists(name, Folder.PSEUDO_CLASS));
    }

    public String getName() {
        return name;
    }

    public boolean isRemotelyAccessible() {
        return remotelyAccessible;
    }

    AppStorage getStorage() {
        return storage;
    }

    public EventsBus getEventBus() {
        return storage.getEventsBus();
    }

    public Folder getRootFolder() {
        return new Folder(new FileCreationContext(rootNodeInfo.get(), storage, this));
    }

    /**
     * Creates a new Node in this file system. This is a low level method, and should seldom be used by the AFS API users.
     */
    public Node createNode(NodeInfo nodeInfo) {
        Objects.requireNonNull(nodeInfo);
        Objects.requireNonNull(data);
        FileCreationContext context = new FileCreationContext(nodeInfo, storage, this);
        if (Folder.PSEUDO_CLASS.equals(nodeInfo.getPseudoClass())) {
            return new Folder(context);
        } else if (Project.PSEUDO_CLASS.equals(nodeInfo.getPseudoClass())) {
            return new Project(context);
        } else {
            FileExtension extension = data.getFileExtensionByPseudoClass(nodeInfo.getPseudoClass());
            if (extension != null) {
                return extension.createFile(context);
            } else {
                return new UnknownFile(context);
            }
        }
    }

    /**
     * Get a project file by its ID.
     */
    public <T extends ProjectFile> T findProjectFile(String projectFileId, Class<T> clazz) {
        Objects.requireNonNull(projectFileId);
        Objects.requireNonNull(clazz);

        // get file info
        NodeInfo projectFileInfo = storage.getNodeInfo(projectFileId);
        Project project = createProject(projectFileInfo);

        // then create the file
        ProjectFile projectFile = project.createProjectFile(projectFileInfo);

        // check file is the right type
        if (!clazz.isAssignableFrom(projectFile.getClass())) {
            throw new AfsException("Project file '" + projectFileId + "' is not a " + clazz.getName()
                    + " instance (" + projectFile.getClass() + ")");
        }

        return (T) projectFile;
    }

    /**
     * Retrieve a project node with undefined class
     * @param nodeId the node Id
     * @throws AfsStorageException if the node not found
     * @return a typed node
     */
    public AbstractNodeBase fetchNode(String nodeId, boolean connected) {
        Objects.requireNonNull(nodeId);

        NodeInfo projectFileInfo = storage.getNodeInfo(nodeId);
        NodeInfo projectInfo = searchProject(projectFileInfo.getId());

        if (projectInfo == null) {
            return createNode(projectFileInfo);
        }
        Project project = new Project(new FileCreationContext(projectInfo, storage, this));

        ProjectFileCreationContext context = new ProjectFileCreationContext(projectFileInfo, storage, project, connected);

        if (ProjectFolder.PSEUDO_CLASS.equals(projectFileInfo.getPseudoClass())) {
            return new ProjectFolder(context);
        }

        ProjectFileExtension extension = data.getProjectFileExtensionByPseudoClass(projectFileInfo.getPseudoClass());
        if (extension != null) {
            return extension.createProjectFile(context);
        }
        return createNode(projectFileInfo);
    }

    /**
     * Retrieve a project node with undefined class
     * @param nodeId the node Id
     * @return a typed node
     */
    public AbstractNodeBase fetchNode(String nodeId) {
        return fetchNode(nodeId, true);
    }

    /**
     * Get a project by its ID
     *
     * @param projectId projectID
     * @return the optionally found project
     */
    public Optional<Project> findProject(String projectId) {
        Objects.requireNonNull(projectId);

        NodeInfo projectInfo = storage.getNodeInfo(projectId);
        if (projectInfo != null && Project.PSEUDO_CLASS.equals(projectInfo.getPseudoClass())) {
            return Optional.of(new Project(new FileCreationContext(projectInfo, storage, this)));
        }

        return Optional.empty();
    }

    /**
     * Get a project folder by its ID.
     */
    public ProjectFolder findProjectFolder(String projectFolderId) {

        Objects.requireNonNull(projectFolderId);
        // get file info
        NodeInfo projectFolderInfo = storage.getNodeInfo(projectFolderId);

        Project project = createProject(projectFolderInfo);

        // then create and return the projectFolder
        return project.createProjectFolder(projectFolderInfo);
    }

    private Project createProject(NodeInfo projectNodeInfo) {
        NodeInfo parentInfo = searchProject(projectNodeInfo.getId());

        if (parentInfo == null) {
            throw new AfsException("Node '" + projectNodeInfo.getId() + " parent project cannot be found.");
        }

        // create the project
        return new Project(new FileCreationContext(parentInfo, storage, this));
    }

    /**
     * Find the project of the node
     * @param nodeId The node id
     * @return Project info or null if not found
     */
    protected NodeInfo searchProject(String nodeId) {
        // walk the node hierarchy until finding a project
        NodeInfo parentInfo = storage.getParentNode(nodeId).orElse(null);
        while (parentInfo != null && !Project.PSEUDO_CLASS.equals(parentInfo.getPseudoClass())) {
            parentInfo = storage.getParentNode(parentInfo.getId()).orElse(null);
        }
        return parentInfo;
    }

    public TaskMonitor getTaskMonitor() {
        return taskMonitor;
    }

    public AppData getData() {
        return data;
    }

    void setData(AppData data) {
        this.data = Objects.requireNonNull(data);
    }

    public boolean isClosed() {
        return storage.isClosed();
    }

    public List<FileSystemCheckIssue> checkFileSystem(FileSystemCheckOptions options) {
        return storage.checkFileSystem(options);
    }

    @Override
    public void close() {
        storage.close();
        taskMonitor.close();
    }
}
