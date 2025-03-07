/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AfsNodeNotFoundException;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.events.NodeEvent;
import com.powsybl.computation.ComputationManager;
import com.powsybl.iidm.network.NetworkFactoryService;
import com.powsybl.timeseries.RegularTimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesDataType;
import com.powsybl.timeseries.TimeSeriesMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.Interval;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class AfsBaseTest {
    private static final String NODE_NOT_FOUND_REGEX = "Node [0-9a-fA-F-]{36} not found";
    private static final Pattern NODE_NOT_FOUND_PATTERN = Pattern.compile(NODE_NOT_FOUND_REGEX);

    private FileSystem fileSystem;

    private AppStorage storage;

    private AppFileSystem afs;

    private AppData appData;

    @BeforeEach
    void setup() {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        ComputationManager computationManager = Mockito.mock(ComputationManager.class);
        appData = new AppData(computationManager, computationManager, Collections.emptyList(),
            Collections.emptyList(), List.of(new FooFileExtension(), new WithDependencyFileExtension()), Collections.emptyList());

        storage = MapDbAppStorage.createMem("mem", appData.getEventsBus());

        afs = new AppFileSystem("mem", true, storage);
        afs.setData(appData);
        appData.addFileSystem(afs);
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
        fileSystem.close();
    }

    @Test
    void appDataTest() {
        assertSame(InMemoryEventsBus.class, appData.getEventsBus().getClass());
        assertSame(afs, appData.getFileSystem("mem"));
        assertNull(appData.getFileSystem("???"));
        assertEquals(Collections.singletonList("mem"), appData.getRemotelyAccessibleFileSystemNames());
        assertNotNull(appData.getRemotelyAccessibleStorage("mem"));
        assertEquals("mem", afs.getName());
        assertEquals(2, appData.getProjectFileClasses().size());
    }

    @Test
    void rootTest() {
        Folder root = afs.getRootFolder();
        assertNotNull(root);
    }

    @Test
    void directoryTest() {
        Folder root = afs.getRootFolder();

        // Create a directory under root
        Folder dir1 = root.createFolder("dir1");
        assertTrue(storage.isConsistent(dir1.getId()));
        assertNotNull(dir1);

        // Check from getting it back from root
        dir1 = root.getFolder("dir1").orElse(null);
        assertNotNull(dir1);
        assertTrue(dir1.isFolder());
        assertTrue(dir1.isWritable());
        assertEquals("dir1", dir1.getName());
        assertNotNull(dir1.getCreationDate());
        assertNotNull(dir1.getModificationDate());
        assertEquals(0, dir1.getVersion());
        assertFalse(dir1.isAheadOfVersion());
        assertEquals(dir1.getName(), dir1.toString());
        assertEquals("mem", dir1.getParent().orElseThrow(AssertionError::new).getName());
    }

    @Test
    void subdirectoriesTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");

        // Create two subdirectories under dir1
        dir1.createFolder("dir2");
        dir1.createFolder("dir3");

        // Check that dir1 has 2 children
        assertEquals(2, dir1.getChildren().size());

        // Tests on dir1/dir2
        Folder dir2 = dir1.getFolder("dir2").orElse(null);
        assertNotNull(dir2);
        assertNotNull(dir2.getParent());
        assertEquals("mem:/dir1", dir2.getParent().orElseThrow(AssertionError::new).getPath().toString());
        String str = dir2.getPath().toString();
        assertEquals("mem:/dir1/dir2", str);

        // One way to get to dir1/dir2
        Folder mayBeDir2 = afs.getRootFolder().getFolder("dir1/dir2").orElse(null);
        assertNotNull(mayBeDir2);
        assertEquals("dir2", mayBeDir2.getName());

        // Another way to get to dir1/dir2
        Folder mayBeDir2otherWay = afs.getRootFolder().getChild(Folder.class, "dir1", "dir2").orElse(null);
        assertNotNull(mayBeDir2otherWay);
        assertEquals("dir2", mayBeDir2otherWay.getName());

        // dir3 doest not exist but dir1/dir3 does
        Folder dir3 = root.getFolder("dir3").orElse(null);
        assertNull(dir3);
        dir3 = root.getFolder("dir1/dir3").orElse(null);
        assertNotNull(dir3);
    }

    @Test
    void projectTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Folder dir2 = dir1.createFolder("dir2");

        // Create a project under dir1/dir2
        Project project1 = dir2.createProject("project1");
        project1.setDescription("test project");
        assertNotNull(project1);
        assertEquals("project1", project1.getName());
        assertEquals("test project", project1.getDescription());

        // Check the project parent
        assertNotNull(project1.getParent());
        assertEquals("mem:/dir1/dir2", project1.getParent().orElseThrow(AssertionError::new).getPath().toString());

        // Check that the root folder of the project is empty
        assertTrue(project1.getRootFolder().getChildren().isEmpty());

        // Check the file system
        assertSame(project1.getFileSystem(), afs);
    }

    @Test
    void renameProjectTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Project project = dir1.createProject("project2");
        project.rename("project22");
        assertEquals("project22", project.getName());
    }

    @Test
    void renameProjectExceptionTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");

        // Create a first project called project1
        dir1.createProject("project1");

        // Create another project and try to rename it project1
        Project project = dir1.createProject("project2");
        AfsException exception = assertThrows(AfsException.class, () -> project.rename("project1"));
        assertEquals("name already exists", exception.getMessage());
    }

    @Test
    void deleteProjectTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");

        // Create a project
        Project project = dir1.createProject("project3");
        assertFalse(dir1.getChildren().isEmpty());

        // Delete the project
        project.delete();
        assertTrue(dir1.getChildren().isEmpty());
    }

    @Test
    void deleteNonEmptyFolderTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");

        // Create a project
        dir1.createProject("project5");
        AfsException exception = assertThrows(AfsException.class, dir1::delete, "non-empty folders can not be deleted");
        assertEquals("non-empty folders can not be deleted", exception.getMessage());
    }

    @Test
    void moveProjectTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Folder dir2 = root.createFolder("dir2");

        // Create a project in dir1
        Project project = dir1.createProject("projet4");

        // Check the directories content
        assertFalse(dir1.getChildren().isEmpty());
        assertTrue(dir2.getChildren().isEmpty());

        // Move the project
        project.moveTo(dir2);

        // Check the directories content
        assertTrue(dir1.getChildren().isEmpty());
        assertFalse(dir2.getChildren().isEmpty());
    }

    @Test
    void createFolderInProjectTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Project project = dir1.createProject("projet4");
        ProjectFolder rootFolder = project.getRootFolder();

        // Create a new folder inside the root folder
        ProjectFolder dir2 = rootFolder.createFolder("dir2");

        // Some checks on the new folder
        assertTrue(dir2.isFolder());
        assertEquals("dir2", dir2.getName());
        assertNotNull(dir2.getParent());
        assertTrue(dir2.getChildren().isEmpty());
        assertEquals(1, rootFolder.getChildren().size());

        // Remove the new folder
        dir2.delete();
        assertTrue(rootFolder.getChildren().isEmpty());
        AfsNodeNotFoundException exception = assertThrows(AfsNodeNotFoundException.class, dir2::getChildren);
        assertTrue(NODE_NOT_FOUND_PATTERN.matcher(exception.getMessage()).matches());

        // Create new folders
        ProjectFolder dir5 = rootFolder.createFolder("dir5");
        ProjectFolder dir6 = dir5.createFolder("dir6");
        assertEquals(List.of("dir5", "dir6"), dir6.getPath().toList().subList(1, 3));
        assertEquals("dir5/dir6", dir6.getPath().toString());
        assertEquals("dir6", rootFolder.getChild("dir5/dir6").orElseThrow(AssertionError::new).getName());
    }

    @Test
    void moveFolderInChildFolderTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Folder dir2 = dir1.createFolder("dir2");
        AfsException exception = assertThrows(AfsException.class, () -> dir1.moveTo(dir2));
        assertEquals("The source node is an ancestor of the target node", exception.getMessage());
    }

    @Test
    void ancestryTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Folder dir2 = dir1.createFolder("dir2");
        assertTrue(dir1.isParentOf(dir2));
        assertTrue(root.isAncestorOf(dir2));
        dir2.moveTo(dir1); // Does nothing
        assertTrue(dir1.isParentOf(dir2));
        assertTrue(root.isAncestorOf(dir2));
    }

    @Test
    void listenerTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Project project = dir1.createProject("projet4");
        // Configure listeners
        List<String> added = new ArrayList<>();
        List<String> removed = new ArrayList<>();
        ProjectFolderListener l = new ProjectFolderListener() {
            @Override
            public void childAdded(String nodeId) {
                added.add(nodeId);
            }

            @Override
            public void childRemoved(String nodeId) {
                removed.add(nodeId);
            }
        };

        // Add the listener to the project's root folder
        ProjectFolder rootFolder = project.getRootFolder();
        rootFolder.addListener(l);

        // Create a new folder inside the root folder
        ProjectFolder dir2 = rootFolder.createFolder("dir2");

        // Remove the new folder
        dir2.delete();

        // Create a new folder
        ProjectFolder dir3 = rootFolder.createFolder("dir3");

        // Check the listeners
        assertEquals(Arrays.asList(dir2.getId(), dir3.getId()), added);
        assertEquals(Collections.singletonList(dir2.getId()), removed);
    }

    @Test
    void renameProjectFolderTest() {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Project project = dir1.createProject("projet1");
        ProjectFolder rootFolder = project.getRootFolder();

        ProjectFolder dir2 = rootFolder.createFolder("dir2");
        dir2.rename("dir22");
        assertEquals("dir22", dir2.getName());
    }

    @Test
    void archivingTest() throws IOException {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Project project = dir1.createProject("projet1");
        ProjectFolder rootFolder = project.getRootFolder();
        ProjectFolder dir2 = rootFolder.createFolder("dir2");
        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectories(rootDir);

        // Archive
        dir2.archive(rootDir);
        Path child = rootDir.resolve(dir2.getId());
        assertTrue(Files.exists(child));

        // Unarchive
        ProjectFolder dir8 = rootFolder.createFolder("dir8");
        assertEquals(0, dir8.getChildren().size());
        dir8.unarchive(child);
        assertEquals(1, dir8.getChildren().size());
        assertEquals("dir2", dir8.getChildren().get(0).getName());
    }

    @Test
    void archiveExceptionTest() throws IOException {
        Folder root = afs.getRootFolder();
        Folder dir1 = root.createFolder("dir1");
        Project project = dir1.createProject("projet1");
        ProjectFolder rootFolder = project.getRootFolder();
        ProjectFolder dir2 = rootFolder.createFolder("dir2");
        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectories(rootDir);

        // Archive a non-existing folder
        Path testDirNotExists = rootDir.resolve("testDirNotExists");
        assertThrows(UncheckedIOException.class, () -> dir2.archive(testDirNotExists));

        ProjectFolder dir3 = rootFolder.createFolder("dir3");
        AfsException exception = assertThrows(AfsException.class, () -> dir3.findService(NetworkFactoryService.class));
        assertEquals("No service found for class interface com.powsybl.iidm.network.NetworkFactoryService", exception.getMessage());
    }

    @Test
    void archiveAndUnarchiveTestWithZip() throws IOException {
        Project project = afs.getRootFolder().createProject("test");
        ProjectFolder rootFolder = project.getRootFolder();
        ProjectFolder dir1 = rootFolder.createFolder("dir1");
        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectory(rootDir);
        Files.createDirectory(rootDir.resolve("test"));
        dir1.archive(rootDir.resolve("test"), true, false, new HashMap<>());
        Path child = rootDir.resolve("test.zip");
        assertTrue(Files.exists(child));

        ProjectFolder dir2 = rootFolder.createFolder("dir2");
        dir2.unarchive(child, true);
        assertEquals(1, dir2.getChildren().size());
    }

    @Test
    void archiveAndUnarchiveTestWithDirAndBlackList() throws IOException {
        Project project = afs.getRootFolder().createProject("test");
        ProjectFolder rootFolder = project.getRootFolder();
        ProjectFolder dir1 = rootFolder.createFolder("dir1");
        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectory(rootDir);

        // Write two files associated with the same node
        try (Writer ignored = new OutputStreamWriter(storage.writeBinaryData(dir1.getId(), "data1"));
            Writer ignored1 = new OutputStreamWriter(storage.writeBinaryData(dir1.getId(), "data2"))) {
            // Nothing to write inside, we just want to create the files
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Map<String, List<String>> blackList = new HashMap<>();
        List<String> deleteExtension = new ArrayList<>();
        deleteExtension.add("data1");
        blackList.put("projectFolder", deleteExtension);
        dir1.archive(rootDir, blackList);
        Path child = rootDir.resolve(dir1.getId());
        assertTrue(Files.exists(child));

        ProjectFolder dir2 = rootFolder.createFolder("dir2");
        dir2.unarchive(child, false);
        assertEquals(1, dir2.getChildren().size());
    }

    @Test
    void archiveAndUnarchiveTestWithDependency() throws IOException {
        /* In this test, there are two directories each with a file
           There is a dependency between the first and the second file
           Test archive and unarchive the first directory with these dependencies */

        Project project = afs.getRootFolder().createProject("test");
        ProjectFolder rootFolder = project.getRootFolder();
        ProjectFolder dir1 = rootFolder.createFolder("dir1");
        ProjectFolder dir2 = rootFolder.createFolder("dir2");

        NodeInfo testDataInfo = storage.createNode(dir1.getId(), "data", "data", "", 0, new NodeGenericMetadata());
        NodeInfo testData2Info = storage.createNode(dir2.getId(), "data2", "data", "", 0, new NodeGenericMetadata().setString("s1", "v1")
            .setDouble("d1", 1d)
            .setInt("i1", 2)
            .setBoolean("b1", false));
        storage.setConsistent(testDataInfo.getId());
        storage.setConsistent(testData2Info.getId());
        storage.addDependency(testDataInfo.getId(), "mylink2", testData2Info.getId());

        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectory(rootDir);
        Files.createDirectory(rootDir.resolve("test"));
        dir1.archive(rootDir.resolve("test"), true, true, new HashMap<>());
        Path child = rootDir.resolve("test.zip");
        assertTrue(Files.exists(child));

        ProjectFolder dir3 = rootFolder.createFolder("dir3");
        dir3.unarchive(child, true);
        assertEquals(1, dir3.getChildren().size());
        ProjectNode node1 = dir3.getChildren().get(0);
        List<NodeInfo> listNode = storage.getChildNodes(node1.getId());
        assertEquals(2, listNode.size());
        assertEquals("data", listNode.get(0).getName());
        assertEquals("dir2", listNode.get(1).getName());
        assertEquals("data2", storage.getChildNodes(listNode.get(1).getId()).get(0).getName());
    }

    @Test
    void archiveAndUnarchiveTestRemoveTS() throws IOException {
        Project project = afs.getRootFolder().createProject("test");
        ProjectFolder rootFolder = project.getRootFolder();
        ProjectFolder dir1 = rootFolder.createFolder("dir1");
        NodeInfo metrix = storage.createNode(dir1.getId(), "metrix", "METRIX", "", 0, new NodeGenericMetadata());
        NodeInfo virtualTimeSeries = storage.createNode(dir1.getId(), "virtualTimeSeries", "TSV", "", 0, new NodeGenericMetadata());

        RegularTimeSeriesIndex index = RegularTimeSeriesIndex.create(Interval.parse("2015-01-01T00:00:00Z/2015-01-01T01:45:00Z"),
            Duration.ofMinutes(15));
        storage.createTimeSeries(metrix.getId(), new TimeSeriesMetadata("ts1", TimeSeriesDataType.STRING, index));
        storage.createTimeSeries(virtualTimeSeries.getId(), new TimeSeriesMetadata("ts2", TimeSeriesDataType.DOUBLE, index));

        storage.setConsistent(metrix.getId());
        storage.setConsistent(virtualTimeSeries.getId());
        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectory(rootDir);
        Files.createDirectory(rootDir.resolve("test"));
        List<String> deleteTSPseudoClass = new ArrayList<>();
        deleteTSPseudoClass.add("METRIX");
        dir1.archive(rootDir.resolve("test"), false, true, new HashMap<>(), deleteTSPseudoClass);
        Path child1 = rootDir.resolve("test/" + dir1.getId() + "/children/" + metrix.getId() + "/time-series/ts1.ts");
        Path child2 = rootDir.resolve("test/" + dir1.getId() + "/children/" + virtualTimeSeries.getId() + "/time-series/ts2.ts");
        assertTrue(Files.exists(child2));
        assertTrue(Files.notExists(child1));
    }

    @Test
    void archiveAndUnarchiveTestWithDependencies() throws IOException {

        /* In this test, there are two directories, one with a file and the second with two files
           There is a dependency between the first and the second file and between the second an the third
           Test archive and unarchive the first directory with these dependencies */

        Project project = afs.getRootFolder().createProject("test");
        ProjectFolder rootFolder = project.getRootFolder();
        ProjectFolder dir1 = rootFolder.createFolder("dir1");
        ProjectFolder dir2 = rootFolder.createFolder("dir2");

        NodeInfo testDataInfo = storage.createNode(dir1.getId(), "data", "data", "", 0, new NodeGenericMetadata());
        NodeInfo testData2Info = storage.createNode(dir2.getId(), "data2", "data", "", 0, new NodeGenericMetadata().setString("s1", "v1")
            .setDouble("d1", 1d)
            .setInt("i1", 2)
            .setBoolean("b1", false));
        NodeInfo testData3Info = storage.createNode(dir2.getId(), "data3", "data", "", 0, new NodeGenericMetadata().setString("s1", "v1")
            .setDouble("d1", 1d)
            .setInt("i1", 2)
            .setBoolean("b1", false));
        storage.setConsistent(testDataInfo.getId());
        storage.setConsistent(testData2Info.getId());
        storage.setConsistent(testData3Info.getId());
        storage.addDependency(testDataInfo.getId(), "mylink", testData2Info.getId());
        storage.addDependency(testData2Info.getId(), "mylink2", testData3Info.getId());

        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectory(rootDir);
        Files.createDirectory(rootDir.resolve("test"));
        dir1.archive(rootDir.resolve("test"), true, true, new HashMap<>());
        Path child = rootDir.resolve("test.zip");
        assertTrue(Files.exists(child));

        ProjectFolder dir3 = rootFolder.createFolder("dir3");
        dir3.unarchive(child, true);
        assertEquals(1, dir3.getChildren().size());
        ProjectNode node1 = dir3.getChildren().get(0);
        List<NodeInfo> listNode = storage.getChildNodes(node1.getId());
        assertEquals(2, listNode.size());
        assertEquals("data", listNode.get(0).getName());
        assertEquals("dir2", listNode.get(1).getName());
        assertEquals(2, storage.getChildNodes(listNode.get(1).getId()).size());
        assertEquals("data2", storage.getChildNodes(listNode.get(1).getId()).get(0).getName());
        assertEquals("data3", storage.getChildNodes(listNode.get(1).getId()).get(1).getName());
    }

    @Test
    void archiveAndUnarchiveTestWithDeepDependencies() throws IOException {

        /* In this test, there are two directories, one with two files and the second with one file
           There is a dependency between the first and the second file and between the second an the third
           Test archive and unarchive the first directory with these dependencies */

        Project project = afs.getRootFolder().createProject("test");
        ProjectFolder rootFolder = project.getRootFolder();
        ProjectFolder dir1 = rootFolder.createFolder("dir1");
        ProjectFolder dir2 = rootFolder.createFolder("dir2");

        NodeInfo testDataInfo = storage.createNode(dir1.getId(), "data", "data", "", 0, new NodeGenericMetadata());
        NodeInfo testData2Info = storage.createNode(dir1.getId(), "data2", "data", "", 0, new NodeGenericMetadata().setString("s1", "v1")
            .setDouble("d1", 1d)
            .setInt("i1", 2)
            .setBoolean("b1", false));
        NodeInfo testData3Info = storage.createNode(dir2.getId(), "data3", "data", "", 0, new NodeGenericMetadata().setString("s1", "v1")
            .setDouble("d1", 1d)
            .setInt("i1", 2)
            .setBoolean("b1", false));
        storage.setConsistent(testDataInfo.getId());
        storage.setConsistent(testData2Info.getId());
        storage.setConsistent(testData3Info.getId());
        storage.addDependency(testDataInfo.getId(), "mylink", testData2Info.getId());
        storage.addDependency(testData2Info.getId(), "mylink2", testData3Info.getId());

        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectory(rootDir);
        Files.createDirectory(rootDir.resolve("test"));
        dir1.archive(rootDir.resolve("test"), true, true, new HashMap<>());
        Path child = rootDir.resolve("test.zip");
        assertTrue(Files.exists(child));

        ProjectFolder dir3 = rootFolder.createFolder("dir3");
        dir3.unarchive(child, true);
        assertEquals(1, dir3.getChildren().size());
        ProjectNode node1 = dir3.getChildren().get(0);
        List<NodeInfo> listNode = storage.getChildNodes(node1.getId());
        assertEquals(3, listNode.size());
        assertEquals("data", listNode.get(0).getName());
        assertEquals("data2", listNode.get(1).getName());
        assertEquals("dir2", listNode.get(2).getName());
        assertEquals(1, storage.getChildNodes(listNode.get(2).getId()).size());
        assertEquals("data3", storage.getChildNodes(listNode.get(2).getId()).get(0).getName());
    }

    @Test
    void moveToTest() {
        Project project = afs.getRootFolder().createProject("test");
        ProjectFolder test1 = project.getRootFolder().createFolder("test1");
        ProjectFolder test2 = project.getRootFolder().createFolder("test2");
        FooFile file = test1.fileBuilder(FooFileBuilder.class)
            .withName("foo")
            .build();
        assertEquals(test1.getId(), file.getParent().orElseThrow(AssertionError::new).getId());
        assertEquals(1, test1.getChildren().size());
        assertTrue(test2.getChildren().isEmpty());
        file.moveTo(test2);
        assertEquals(test2.getId(), file.getParent().orElseThrow(AssertionError::new).getId());
        assertTrue(test1.getChildren().isEmpty());
        assertEquals(1, test2.getChildren().size());
    }

    @Test
    void findProjectFolderTest() {

        Project project = afs.getRootFolder().createProject("test");
        ProjectFolder test1 = project.getRootFolder().createFolder("test1");
        ProjectFolder projectFolderResult = afs.findProjectFolder(test1.getId());
        assertNotNull(projectFolderResult);
        assertEquals(test1.getId(), projectFolderResult.getId());
        assertEquals(test1.getParentInfo(), projectFolderResult.getParentInfo());
        assertEquals(test1.getCreationDate(), projectFolderResult.getCreationDate());
        assertEquals(test1.getModificationDate(), projectFolderResult.getModificationDate());

    }

    @Test
    void findProjectFileTest() {
        Project project = afs.getRootFolder().createProject("test");
        FooFile createdFile = project.getRootFolder().fileBuilder(FooFileBuilder.class)
            .withName("foo")
            .build();
        ProjectFile foundFile = afs.findProjectFile(createdFile.getId(), FooFile.class);
        assertNotNull(foundFile);
        assertEquals(createdFile.getId(), foundFile.getId());
        assertEquals(createdFile.getName(), foundFile.getName());
        assertEquals(createdFile.getDescription(), foundFile.getDescription());
        assertEquals(createdFile.getCreationDate(), foundFile.getCreationDate());
        assertEquals(createdFile.getModificationDate(), foundFile.getModificationDate());
        assertEquals(createdFile.getFileSystem(), foundFile.getFileSystem());
        assertEquals(createdFile.getDependencies(), foundFile.getDependencies());
        assertEquals(createdFile.getCodeVersion(), foundFile.getCodeVersion());
    }

    @Test
    void findProjectTest() {
        Project project = afs.getRootFolder().createProject("test");
        Project foundProject = afs.findProject(project.getId()).orElse(null);
        assertNotNull(foundProject);
        assertEquals(project.getId(), foundProject.getId());
        assertEquals(project.getName(), foundProject.getName());
        assertEquals(project.getDescription(), foundProject.getDescription());
        assertEquals(project.getCreationDate(), foundProject.getCreationDate());
        assertEquals(project.getModificationDate(), foundProject.getModificationDate());
        assertEquals(project.getFileSystem(), foundProject.getFileSystem());
        assertEquals(project.getCodeVersion(), foundProject.getCodeVersion());
    }

    @Test
    void fetchNodeTest() {
        Folder folder = afs.getRootFolder().createFolder("testFolder");
        Project project = folder.createProject("test");
        FooFile createdFile = project.getRootFolder().fileBuilder(FooFileBuilder.class)
            .withName("foo")
            .build();
        ProjectFolder projectFolder = project.getRootFolder().createFolder("testFolder");
        FooFile nestedFile = projectFolder.fileBuilder(FooFileBuilder.class)
            .withName("bar")
            .build();

        BiConsumer<AbstractNodeBase<?>, AbstractNodeBase<?>> checkResult = (source, result) -> {
            assertNotNull(result);
            assertEquals(source.getClass(), result.getClass());
            assertEquals(source.getId(), result.getId());
            assertEquals(source.getName(), result.getName());
            assertEquals(source.getName(), result.getName());
            assertEquals(source.getDescription(), result.getDescription());
            assertEquals(source.getCreationDate(), result.getCreationDate());
            assertEquals(source.getModificationDate(), result.getModificationDate());
            assertEquals(source.getCodeVersion(), result.getCodeVersion());

            if (source instanceof ProjectNode projectNode) {
                assertEquals(projectNode.getProject().getId(), ((ProjectNode) result).getProject().getId());
                assertEquals(projectNode.getFileSystem(), ((ProjectNode) result).getFileSystem());
            }
        };

        checkResult.accept(project, afs.fetchNode(project.getId()));
        checkResult.accept(folder, afs.fetchNode(folder.getId()));
        checkResult.accept(createdFile, afs.fetchNode(createdFile.getId()));
        checkResult.accept(nestedFile, afs.fetchNode(nestedFile.getId()));
        checkResult.accept(projectFolder, afs.fetchNode(projectFolder.getId()));

        String uuid = UUID.randomUUID().toString();
        AfsNodeNotFoundException exception = assertThrows(AfsNodeNotFoundException.class, () -> afs.fetchNode(uuid));
        assertTrue(NODE_NOT_FOUND_PATTERN.matcher(exception.getMessage()).matches());
    }

    @Test
    void hasDeepDependencyTest() {
        Project project = afs.getRootFolder().createProject("test");
        FooFile createdFile = project.getRootFolder().fileBuilder(FooFileBuilder.class)
            .withName("foo")
            .build();
        FooFile otherFile = project.getRootFolder().fileBuilder(FooFileBuilder.class)
            .withName("bar")
            .build();
        createdFile.setDependencies("dep", Collections.singletonList(otherFile));
        assertTrue(createdFile.hasDeepDependency(otherFile));
        assertFalse(createdFile.hasDeepDependency(createdFile));
        otherFile.setDependencies("dep", Collections.singletonList(createdFile));
        assertTrue(createdFile.hasDeepDependency(createdFile, "dep"));
    }

    @Test
    void invalidate() {
        Folder folder = afs.getRootFolder().createFolder("testFolder");
        Project project = folder.createProject("test");
        FooFile fooFile = project.getRootFolder().fileBuilder(FooFileBuilder.class).withName("Foo").build();
        WithDependencyFile fileWithDep = project.getRootFolder().fileBuilder(WithDependencyFileBuilder.class).withName("WithDependencyFile1").build();
        fileWithDep.setFooDependency(fooFile);
        WithDependencyFile fileWithDep2 = project.getRootFolder().fileBuilder(WithDependencyFileBuilder.class).withName("WithDependencyFile2").build();
        fileWithDep2.setFooDependency(fileWithDep);
        String nodeEventType = "type";

        // Previous code add connected WithDependencyFile with listeners
        storage.getEventsBus().flush();
        storage.getEventsBus().removeListeners();

        // We verify that invalidate() instantiate all the WithDependencyFile backward dependencies
        // And we test that they are not connected, so that they don't record events
        List<ProjectFile> dependenciesInvalidated = fooFile.invalidate();
        assertEquals(2, dependenciesInvalidated.size());
        for (ProjectFile projectFile : dependenciesInvalidated) {
            assertInstanceOf(WithDependencyFile.class, projectFile);
            WithDependencyFile withDepFile = (WithDependencyFile) projectFile;
            assertEquals(1, withDepFile.invalidatedTime.get());
            storage.getEventsBus().pushEvent(new NodeEvent("id", nodeEventType) {
            }, "Topic");
            storage.getEventsBus().flush();
            Optional<NodeEvent> updateEvent = withDepFile.events.stream().filter(nodeEvent -> nodeEventType.equals(nodeEvent.getType())).findFirst();
            assertTrue(updateEvent.isEmpty());
        }

        // Here we test the case where the dependency is connected. The event is well recorded
        List<ProjectFile> connectedBackwardDependencies = fooFile.getBackwardDependencies(true);
        NodeEvent event = new NodeEvent("id", nodeEventType) {
        };
        storage.getEventsBus().pushEvent(event, "Topic");
        storage.getEventsBus().flush();
        assertEquals(1, connectedBackwardDependencies.size());
        assertEquals(WithDependencyFile.class, connectedBackwardDependencies.get(0).getClass());
        WithDependencyFile withDependencyFile = (WithDependencyFile) connectedBackwardDependencies.get(0);
        Optional<NodeEvent> updateEvent2 = withDependencyFile.events.stream()
            .filter(nodeEvent -> nodeEventType.equals(nodeEvent.getType()))
            .findFirst();
        assertFalse(updateEvent2.isEmpty());
        assertEquals(event, updateEvent2.get());
    }
}
