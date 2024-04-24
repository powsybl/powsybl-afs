/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.google.common.collect.ImmutableList;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.*;
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
import java.util.*;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class AfsBaseTest {

    private FileSystem fileSystem;

    private AppStorage storage;

    private AppFileSystem afs;

    private AppData ad;

    @BeforeEach
    public void setup() {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        ComputationManager computationManager = Mockito.mock(ComputationManager.class);
        ad = new AppData(computationManager, computationManager, Collections.emptyList(),
                Collections.emptyList(), List.of(new FooFileExtension(), new WithDependencyFileExtension()), Collections.emptyList());

        storage = MapDbAppStorage.createMem("mem", ad.getEventsBus());

        afs = new AppFileSystem("mem", true, storage);
        afs.setData(ad);
        ad.addFileSystem(afs);
    }

    @AfterEach
    public void tearDown() throws Exception {
        storage.close();
        fileSystem.close();
    }

    @Test
    void baseTest() {
        assertSame(InMemoryEventsBus.class, ad.getEventsBus().getClass());
        assertSame(afs, ad.getFileSystem("mem"));
        assertNull(ad.getFileSystem("???"));
        assertEquals(Collections.singletonList("mem"), ad.getRemotelyAccessibleFileSystemNames());
        assertNotNull(ad.getRemotelyAccessibleStorage("mem"));
        assertEquals("mem", afs.getName());
        assertEquals(1, ad.getProjectFileClasses().size());
        Folder root = afs.getRootFolder();
        assertNotNull(root);
        Folder dir1 = root.createFolder("dir1");
        assertTrue(storage.isConsistent(dir1.getId()));
        assertNotNull(dir1);
        dir1.createFolder("dir2");
        dir1.createFolder("dir3");
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
        Folder dir2 = dir1.getFolder("dir2").orElse(null);
        assertNotNull(dir2);
        assertNotNull(dir2.getParent());
        assertEquals("mem:/dir1", dir2.getParent().orElseThrow(AssertionError::new).getPath().toString());
        assertEquals(2, dir1.getChildren().size());
        Folder dir3 = root.getFolder("dir3").orElse(null);
        assertNull(dir3);
        String str = dir2.getPath().toString();
        assertEquals("mem:/dir1/dir2", str);
        Folder mayBeDir2 = afs.getRootFolder().getFolder("dir1/dir2").orElse(null);
        assertNotNull(mayBeDir2);
        assertEquals("dir2", mayBeDir2.getName());
        Folder mayBeDir2otherWay = afs.getRootFolder().getChild(Folder.class, "dir1", "dir2").orElse(null);
        assertNotNull(mayBeDir2otherWay);
        assertEquals("dir2", mayBeDir2otherWay.getName());

        Project project1 = dir2.createProject("project1");
        project1.setDescription("test project");
        assertNotNull(project1);
        assertEquals("project1", project1.getName());
        assertEquals("test project", project1.getDescription());
        assertNotNull(project1.getParent());
        assertEquals("mem:/dir1/dir2", project1.getParent().orElseThrow(AssertionError::new).getPath().toString());
        assertTrue(project1.getRootFolder().getChildren().isEmpty());
        assertSame(project1.getFileSystem(), afs);

        Project project2 = dir2.createProject("project2");
        project2.rename("project22");
        assertEquals("project22", project2.getName());

        dir2.createProject("project5");
        Project project102 = dir2.createProject("project6");
        try {
            project102.rename("project5");
            fail();
        } catch (AfsException ignored) {
        }

        Folder dir41 = dir2.createFolder("dir41");
        Project project3 = dir41.createProject("project3");
        project3.delete();
        assertTrue(dir41.getChildren().isEmpty());

        Folder dir51 = dir2.createFolder("dir51");
        dir51.createProject("project5");
        try {
            dir51.delete();
            fail();
        } catch (AfsException ignored) {
        }

        Folder dir71 = root.createFolder("dir7");
        Project project4 = dir41.createProject("projet4");
        project4.moveTo(dir71);
        assertFalse(dir71.getChildren().isEmpty());

        Folder dir81 = root.createFolder("dir8");
        Folder dir82 = dir81.createFolder("dir9");
        try {
            dir81.moveTo(dir82);
            fail();
        } catch (AfsException ignored) {
        }

        assertTrue(dir81.isParentOf(dir82));
        assertTrue(root.isAncestorOf(dir82));
        dir82.moveTo(dir81); // Does nothing
        assertTrue(dir81.isParentOf(dir82));
        assertTrue(root.isAncestorOf(dir82));

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
        ProjectFolder rootFolder = project1.getRootFolder();
        rootFolder.addListener(l);
        ProjectFolder dir4 = rootFolder.createFolder("dir4");
        assertTrue(dir4.isFolder());
        assertEquals("dir4", dir4.getName());
        assertNotNull(dir4.getParent());
        assertTrue(dir4.getChildren().isEmpty());
        assertEquals(1, rootFolder.getChildren().size());

        dir4.delete();
        assertTrue(rootFolder.getChildren().isEmpty());
        try {
            dir4.getChildren();
            fail();
        } catch (Exception ignored) {
        }

        ProjectFolder dir5 = rootFolder.createFolder("dir5");
        ProjectFolder dir6 = dir5.createFolder("dir6");
        assertEquals(ImmutableList.of("dir5", "dir6"), dir6.getPath().toList().subList(1, 3));
        assertEquals("dir5/dir6", dir6.getPath().toString());
        assertEquals("dir6", rootFolder.getChild("dir5/dir6").orElseThrow(AssertionError::new).getName());

        assertEquals(Arrays.asList(dir4.getId(), dir5.getId()), added);
        assertEquals(Collections.singletonList(dir4.getId()), removed);

        ProjectFolder dir7 = rootFolder.createFolder("dir7");
        dir7.rename("dir77");
        assertEquals("dir77", dir7.getName());

        Path rootDir = fileSystem.getPath("/root");
        try {
            Files.createDirectories(rootDir);
            dir7.archive(rootDir);
        } catch (IOException ignored) {
        }
        Path child = rootDir.resolve(dir7.getId());
        assertTrue(Files.exists(child));

        ProjectFolder dir8 = rootFolder.createFolder("dir8");
        assertEquals(0, dir8.getChildren().size());
        dir8.unarchive(child);
        assertEquals(1, dir8.getChildren().size());
        assertEquals("dir77", dir8.getChildren().get(0).getName());

        Path testDirNotExists = rootDir.resolve("testDirNotExists");
        try {
            dir7.archive(testDirNotExists);
            fail();
            dir8.findService(NetworkFactoryService.class);
            fail();
        } catch (UncheckedIOException ignored) {
        }
    }

    @Test
    void archiveAndUnarchiveTestWithZip() throws IOException {
        Project project = afs.getRootFolder().createProject("test");
        ProjectFolder rootFolder = project.getRootFolder();
        ProjectFolder dir1 = rootFolder.createFolder("dir1");
        Path child = null;
        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectory(rootDir);
        Files.createDirectory(rootDir.resolve("test"));
        dir1.archive(rootDir.resolve("test"), true, false, new HashMap<>());
        child = rootDir.resolve("test.zip");
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
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(dir1.getId(), "data1"));
            Writer writer2 = new OutputStreamWriter(storage.writeBinaryData(dir1.getId(), "data2"))) {
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Path child = null;
        Path rootDir = fileSystem.getPath("/root");
        Files.createDirectory(rootDir);
        Map<String, List<String>> blackList = new HashMap<>();
        List<String> deleteExtension = new ArrayList<>();
        deleteExtension.add("data1");
        blackList.put("projectFolder", deleteExtension);
        dir1.archive(rootDir, blackList);
        child = rootDir.resolve(dir1.getId());
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
        NodeInfo testData2Info = storage.createNode(dir2.getId(), "data2", "data", "", 0,
                new NodeGenericMetadata().setString("s1", "v1")
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
        NodeInfo testData2Info = storage.createNode(dir2.getId(), "data2", "data", "", 0,
                new NodeGenericMetadata().setString("s1", "v1")
                        .setDouble("d1", 1d)
                        .setInt("i1", 2)
                        .setBoolean("b1", false));
        NodeInfo testData3Info = storage.createNode(dir2.getId(), "data3", "data", "", 0,
                new NodeGenericMetadata().setString("s1", "v1")
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
        NodeInfo testData2Info = storage.createNode(dir1.getId(), "data2", "data", "", 0,
                new NodeGenericMetadata().setString("s1", "v1")
                        .setDouble("d1", 1d)
                        .setInt("i1", 2)
                        .setBoolean("b1", false));
        NodeInfo testData3Info = storage.createNode(dir2.getId(), "data3", "data", "", 0,
                new NodeGenericMetadata().setString("s1", "v1")
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

        BiConsumer<AbstractNodeBase, AbstractNodeBase> checkResult = (source, result) -> {
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

        try {
            afs.fetchNode(UUID.randomUUID().toString());
            fail();
        } catch (AfsStorageException e) {
            // ignored
        }
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
    public void invalidate() {
        Folder folder = afs.getRootFolder().createFolder("testFolder");
        Project project = folder.createProject("test");
        FooFile fooFile = project.getRootFolder().fileBuilder(FooFileBuilder.class).withName("Foo").build();
        WithDependencyFile fileWithDep = project.getRootFolder().fileBuilder(WithDependencyFileBuilder.class).build();
        fileWithDep.setFooDependency(fooFile);

        // Previous code add connected WithDependencyFile with listeners
        storage.getEventsBus().flush();
        storage.getEventsBus().removeListeners();

        fooFile.invalidate();

        // WithDependencyFile is not connected => don't store event
        String nodeEventType = "type";
        storage.getEventsBus().pushEvent(new NodeEvent("id", nodeEventType) {
        }, "Topic");
        storage.getEventsBus().flush();
        Optional<NodeEvent> updateEvent = fileWithDep.events.stream().filter(nodeEvent -> nodeEventType.equals(nodeEvent.getType())).findFirst();
        assertTrue(updateEvent.isEmpty());

        // WithDependencyFile is not connected => store event
        List<ProjectFile> connectedBackwardDependencies = fooFile.getBackwardDependencies(true);
        storage.getEventsBus().pushEvent(new NodeEvent("id", nodeEventType) {
        }, "Topic");
        storage.getEventsBus().flush();
        assertEquals(1, connectedBackwardDependencies.size());
        assertEquals(WithDependencyFile.class, connectedBackwardDependencies.get(0).getClass());
        WithDependencyFile withDependencyFile = (WithDependencyFile) connectedBackwardDependencies.get(0);
        Optional<NodeEvent> updateEvent2 = withDependencyFile.events.stream()
                .filter(nodeEvent -> nodeEventType.equals(nodeEvent.getType()))
                .findFirst();
        assertFalse(updateEvent2.isEmpty());
    }
}
