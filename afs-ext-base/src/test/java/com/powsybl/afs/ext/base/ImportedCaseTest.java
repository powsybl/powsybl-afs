/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.google.common.collect.ImmutableMap;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.re2j.Pattern;
import com.powsybl.afs.AbstractProjectFileTest;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.FileExtension;
import com.powsybl.afs.Folder;
import com.powsybl.afs.Project;
import com.powsybl.afs.ProjectFileExtension;
import com.powsybl.afs.ProjectFolder;
import com.powsybl.afs.ProjectNode;
import com.powsybl.afs.ServiceExtension;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AfsNodeNotFoundException;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.iidm.network.DefaultNetworkListener;
import com.powsybl.iidm.network.ExportersLoader;
import com.powsybl.iidm.network.ExportersLoaderList;
import com.powsybl.iidm.network.ImportConfig;
import com.powsybl.iidm.network.ImportersLoader;
import com.powsybl.iidm.network.ImportersLoaderList;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.NetworkListener;
import com.powsybl.iidm.serde.XMLExporter;
import com.powsybl.iidm.serde.XMLImporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class ImportedCaseTest extends AbstractProjectFileTest {

    private static final String NODE_NOT_FOUND_REGEX = "Node [0-9a-fA-F-]{36} not found";
    private static final Pattern NODE_NOT_FOUND_PATTERN = Pattern.compile(NODE_NOT_FOUND_REGEX);
    private FileSystem fileSystem;

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    private ExportersLoader createExportersLoader() {
        return new ExportersLoaderList(new XMLExporter());
    }

    private ImportersLoader createImportersLoader() {
        return new ImportersLoaderList(Arrays.asList(new TestImporter(network), new XMLImporter()), Collections.emptyList());
    }

    @Override
    protected List<FileExtension> getFileExtensions() {
        return List.of(new CaseExtension(createImportersLoader()));
    }

    @Override
    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return List.of(new ImportedCaseExtension(createExportersLoader(), createImportersLoader(), new ImportConfig()));
    }

    @Override
    protected List<ServiceExtension> getServiceExtensions() {
        return List.of(new LocalNetworkCacheServiceExtension());
    }

    @Override
    @BeforeEach
    public void setup() throws IOException {
        super.setup();
        NodeInfo rootFolderInfo = storage.createRootNodeIfNotExists("root", Folder.PSEUDO_CLASS);

        NodeInfo nodeInfo = storage.createNode(rootFolderInfo.getId(), "network", Case.PSEUDO_CLASS, "Test format", Case.VERSION,
            new NodeGenericMetadata().setString("format", TestImporter.FORMAT));
        storage.setConsistent(nodeInfo.getId());

        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        Files.createFile(fileSystem.getPath("/work/network.tst"));
    }

    @Override
    @AfterEach
    public void tearDown() throws IOException {
        fileSystem.close();

        super.tearDown();
    }

    @Test
    void caseExistsTest() {
        Folder root = afs.getRootFolder();

        // check case exist
        assertEquals(1, root.getChildren().size());
        assertInstanceOf(Case.class, root.getChildren().get(0));
        Case aCase = (Case) root.getChildren().get(0);
        assertEquals("network", aCase.getName());
        assertEquals("Test format", aCase.getDescription());
        assertFalse(aCase.isFolder());
    }

    @Test
    void importCaseInProject() {
        Folder root = afs.getRootFolder();
        Case aCase = (Case) root.getChildren().get(0);

        // create project end project folder
        Project project = root.createProject("project");
        ProjectFolder folder = project.getRootFolder().createFolder("folder");

        // Create a builder
        ImportedCaseBuilder builder = folder.fileBuilder(ImportedCaseBuilder.class);

        // Building the case does not wirk when parameters are missing
        AfsException exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Case or data source is not set", exception.getMessage());

        // Build the ImportedCase
        ImportedCase importedCase = builder
            .withCase(aCase)
            .withParameter("param1", "true")
            .withParameters(ImmutableMap.of("param2", "1"))
            .build();

        // Check the ImportedCase
        assertNotNull(importedCase);
        assertFalse(importedCase.isFolder());
        assertNotNull(importedCase.getNetwork());
        assertTrue(importedCase.getDependencies().isEmpty());
    }

    @Test
    void listenerTest() {
        Folder root = afs.getRootFolder();
        Case aCase = (Case) root.getChildren().get(0);
        Project project = root.createProject("project");
        ProjectFolder folder = project.getRootFolder().createFolder("folder");
        ImportedCase importedCase = folder.fileBuilder(ImportedCaseBuilder.class)
            .withCase(aCase)
            .withParameter("param1", "true")
            .withParameters(ImmutableMap.of("param2", "1"))
            .build();

        // Mock a listener
        NetworkListener mockedListener = mock(DefaultNetworkListener.class);

        // Get the network and add the listener to it
        assertNotNull(importedCase.getNetwork(Collections.singletonList(mockedListener)));

        // Update something in the network
        network.getSubstation("s1").setTso("tso_new");

        // Check that the listener saw it
        verify(mockedListener, times(1))
            .onUpdate(network.getSubstation("s1"), "tso", null, "TSO", "tso_new");
    }

    @Test
    void queryNetworkTest() {
        Folder root = afs.getRootFolder();
        Case aCase = (Case) root.getChildren().get(0);
        Project project = root.createProject("project");
        ProjectFolder folder = project.getRootFolder().createFolder("folder");
        ImportedCase importedCase = folder.fileBuilder(ImportedCaseBuilder.class)
            .withCase(aCase)
            .withParameter("param1", "true")
            .withParameters(ImmutableMap.of("param2", "1"))
            .build();

        // test network query
        assertEquals("[\"s1\"]", importedCase.queryNetwork(ScriptType.GROOVY, "network.substations.collect { it.id }"));
    }

    @Test
    void reloadImportedCaseTest() {
        Folder root = afs.getRootFolder();
        Case aCase = (Case) root.getChildren().get(0);
        Project project = root.createProject("project");
        ProjectFolder folder = project.getRootFolder().createFolder("folder");
        folder.fileBuilder(ImportedCaseBuilder.class)
            .withCase(aCase)
            .withParameter("param1", "true")
            .withParameters(ImmutableMap.of("param2", "1"))
            .build();
        assertEquals(1, folder.getChildren().size());

        // try to reload the imported case
        ProjectNode projectNode = folder.getChildren().get(0);
        assertNotNull(projectNode);
        assertInstanceOf(ImportedCase.class, projectNode);
        ImportedCase importedCase2 = (ImportedCase) projectNode;
        assertEquals(TestImporter.FORMAT, importedCase2.getImporter().getFormat());
        assertEquals(2, importedCase2.getParameters().size());
        assertEquals("true", importedCase2.getParameters().getProperty("param1"));
    }

    @Test
    void checkChildByNameTest() {
        Folder root = afs.getRootFolder();
        Case aCase = (Case) root.getChildren().get(0);
        Project project = root.createProject("project");
        ProjectFolder folder = project.getRootFolder().createFolder("folder");
        folder.fileBuilder(ImportedCaseBuilder.class)
            .withCase(aCase)
            .withParameter("param1", "true")
            .withParameters(ImmutableMap.of("param2", "1"))
            .build();

        assertTrue(folder.getChild(ImportedCase.class, "network").isPresent());
    }

    @Test
    void deletedImportedCaseTest() {
        Folder root = afs.getRootFolder();
        Case aCase = (Case) root.getChildren().get(0);
        Project project = root.createProject("project");
        ProjectFolder folder = project.getRootFolder().createFolder("folder");
        folder.fileBuilder(ImportedCaseBuilder.class)
            .withCase(aCase)
            .withParameter("param1", "true")
            .withParameters(ImmutableMap.of("param2", "1"))
            .build();
        ProjectNode projectNode = folder.getChildren().get(0);

        // Delete the imported case
        projectNode.delete();
        assertTrue(folder.getChildren().isEmpty());
        String deletedNodeId = projectNode.getId();
        AfsNodeNotFoundException deletedNodeException = assertThrows(AfsNodeNotFoundException.class, () -> storage.getNodeInfo(deletedNodeId));
        assertTrue(NODE_NOT_FOUND_PATTERN.matcher(deletedNodeException.getMessage()).matches());
    }

    @Test
    void testFile() {
        Folder root = afs.getRootFolder();

        // create project
        Project project = root.createProject("project");
        assertNotNull(project);

        // create project folder
        ProjectFolder folder = project.getRootFolder().createFolder("folder");
        assertTrue(folder.getChildren().isEmpty());

        ImportedCase importedCase = folder.fileBuilder(ImportedCaseBuilder.class)
            .withFile(fileSystem.getPath("/work/network.tst"))
            .withName("test")
            .build();
        assertNotNull(importedCase);
        assertEquals("test", importedCase.getName());

        ImportedCase importedCase2 = folder.fileBuilder(ImportedCaseBuilder.class)
            .withFile(fileSystem.getPath("/work/network.tst"))
            .build();
        assertNotNull(importedCase2);
        assertEquals("network", importedCase2.getName());
    }

    @Test
    void testNetwork() {
        Folder root = afs.getRootFolder();

        // create project
        Project project = root.createProject("project");
        assertNotNull(project);

        // create project folder
        ProjectFolder folder = project.getRootFolder().createFolder("folder");
        assertTrue(folder.getChildren().isEmpty());

        Network network = Network.create("NetworkID", "scripting");
        ImportedCase importedCase1 = folder.fileBuilder(ImportedCaseBuilder.class)
            .withName("test")
            .withNetwork(network)
            .build();
        assertNotNull(importedCase1);
        assertEquals("test", importedCase1.getName());

        ImportedCase importedCase2 = folder.fileBuilder(ImportedCaseBuilder.class)
            .withNetwork(network)
            .build();
        assertNotNull(importedCase2);
        assertEquals("NetworkID", importedCase2.getName());
    }
}
