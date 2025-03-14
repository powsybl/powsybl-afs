/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.powsybl.afs.storage.events.*;
import com.powsybl.commons.datasource.DataSource;
import com.powsybl.timeseries.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.Interval;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public abstract class AbstractAppStorageTest {

    protected static final String FOLDER_PSEUDO_CLASS = "folder";
    static final String DATA_FILE_CLASS = "data";

    protected AppStorage storage;

    protected BlockingQueue<NodeEvent> eventStack;

    protected AppStorageListener l = eventList -> eventStack.addAll(eventList.getEvents());

    protected abstract AppStorage createStorage();

    @BeforeEach
    public void setUp() throws Exception {
        eventStack = new LinkedBlockingQueue<>();
        this.storage = createStorage();
        this.storage.getEventsBus().addListener(l);
    }

    @AfterEach
    public void tearDown() {
        if (storage.isClosed()) {

            storage.close();
        }
    }

    private void assertEventStack(NodeEvent... events) throws InterruptedException {
        for (NodeEvent event : events) {
            assertEquals(event, eventStack.take());
        }

        // assert all events have been checked
        assertTrue(eventStack.isEmpty());
    }

    private void assertEventStackInAnyOrder(NodeEvent... expectedEvents) throws InterruptedException {
        List<NodeEvent> collectedEvents = new ArrayList<>();
        for (int i = 0; i < expectedEvents.length; i++) {
            NodeEvent event = eventStack.poll(1, TimeUnit.SECONDS);
            if (event == null) {
                break;
            }
            collectedEvents.add(event);
        }
        assertThat(collectedEvents)
            .containsExactlyInAnyOrder(expectedEvents);

        // assert all events have been checked
        assertTrue(eventStack.isEmpty());
    }

    private void discardEvents(int count) throws InterruptedException {
        for (int i = 0; i < count; i++) {
            eventStack.take();
        }
    }

    private NodeInfo assertRootFolderCreation() throws InterruptedException {
        // create root folder
        NodeInfo rootFolderInfo = storage.createRootNodeIfNotExists(storage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        storage.flush();

        // Assert node consistency
        assertTrue(storage.isConsistent(rootFolderInfo.getId()));

        // check event
        assertEventStack(new NodeCreated(rootFolderInfo.getId(), null), new NodeConsistent(rootFolderInfo.getId()));

        assertNotNull(rootFolderInfo);

        // assert root folder is writable
        assertTrue(storage.isWritable(rootFolderInfo.getId()));

        // assert root folder parent is null
        assertFalse(storage.getParentNode(rootFolderInfo.getId()).isPresent());

        // check root folder name and pseudo class is correct
        assertEquals(storage.getFileSystemName(), storage.getNodeInfo(rootFolderInfo.getId()).getName());
        assertEquals(FOLDER_PSEUDO_CLASS, storage.getNodeInfo(rootFolderInfo.getId()).getPseudoClass());

        // assert root folder is empty
        assertTrue(storage.getChildNodes(rootFolderInfo.getId()).isEmpty());

        return rootFolderInfo;
    }

    private NodeInfo assertTestFolderCreation(NodeInfo rootFolderInfo) throws InterruptedException {
        // create a test folder
        NodeInfo testFolderInfo = storage.createNode(rootFolderInfo.getId(), "test", FOLDER_PSEUDO_CLASS, "", 12,
            new NodeGenericMetadata().setString("k", "v"));
        storage.flush();

        assertFalse(storage.isConsistent(testFolderInfo.getId()));
        assertEquals(1, storage.getInconsistentNodes().size());
        assertEquals(testFolderInfo.getId(), storage.getInconsistentNodes().get(0).getId());

        storage.setConsistent(testFolderInfo.getId());
        storage.flush();

        assertTrue(storage.isConsistent(testFolderInfo.getId()));
        assertTrue(storage.getInconsistentNodes().isEmpty());

        // check event
        assertEventStack(new NodeCreated(testFolderInfo.getId(), rootFolderInfo.getId()), new NodeConsistent(testFolderInfo.getId()));

        // assert parent of test folder is root folder
        assertEquals(rootFolderInfo, storage.getParentNode(testFolderInfo.getId()).orElseThrow(AssertionError::new));

        // check test folder infos are corrects
        NodeInfo retrievedTestFolderInfo = storage.getNodeInfo(testFolderInfo.getId());
        assertEquals(testFolderInfo.getId(), retrievedTestFolderInfo.getId());
        assertEquals("test", retrievedTestFolderInfo.getName());
        assertEquals(FOLDER_PSEUDO_CLASS, retrievedTestFolderInfo.getPseudoClass());
        assertEquals(12, retrievedTestFolderInfo.getVersion());
        assertEquals(Collections.singletonMap("k", "v"), retrievedTestFolderInfo.getGenericMetadata().getStrings());
        assertTrue(retrievedTestFolderInfo.getGenericMetadata().getDoubles().isEmpty());
        assertTrue(retrievedTestFolderInfo.getGenericMetadata().getInts().isEmpty());
        assertTrue(retrievedTestFolderInfo.getGenericMetadata().getBooleans().isEmpty());
        assertEquals("", retrievedTestFolderInfo.getDescription());
        assertTrue(retrievedTestFolderInfo.getCreationTime() > 0);
        assertTrue(retrievedTestFolderInfo.getModificationTime() > 0);

        // check test folder is empty
        assertTrue(storage.getChildNodes(testFolderInfo.getId()).isEmpty());

        // check root folder has one child (test folder)
        assertEquals(1, storage.getChildNodes(rootFolderInfo.getId()).size());
        assertEquals(testFolderInfo, storage.getChildNodes(rootFolderInfo.getId()).get(0));
        assertTrue(storage.getChildNode(rootFolderInfo.getId(), "test").isPresent());
        assertEquals(testFolderInfo, storage.getChildNode(rootFolderInfo.getId(), "test").orElseThrow(AssertionError::new));

        // check getChildNode return null if child does not exist
        assertFalse(storage.getChildNode(rootFolderInfo.getId(), "???").isPresent());

        return testFolderInfo;
    }

    private void assertDescription(NodeInfo testFolderInfo) throws InterruptedException {
        // 3) check description initial value and update
        assertEquals("", testFolderInfo.getDescription());
        storage.setDescription(testFolderInfo.getId(), "hello");
        storage.flush();

        // check event
        assertEventStack(new NodeDescriptionUpdated(testFolderInfo.getId(), "hello"));

        assertEquals("hello", storage.getNodeInfo(testFolderInfo.getId()).getDescription());
    }

    private void assertModificationTimeUpdate(NodeInfo testFolderInfo) {
        // check modification time update
        long oldModificationTime = testFolderInfo.getModificationTime();
        storage.updateModificationTime(testFolderInfo.getId());
        storage.flush();
        assertTrue(storage.getNodeInfo(testFolderInfo.getId()).getModificationTime() >= oldModificationTime);
    }

    private void assertDataNodesCreation(NodeInfo testFolderInfo,
                                         NodeInfo testDataInfo, NodeInfo testData2Info, NodeInfo testData3Info) throws InterruptedException {
        // check events
        assertEventStack(new NodeCreated(testDataInfo.getId(), testFolderInfo.getId()),
            new NodeCreated(testData2Info.getId(), testFolderInfo.getId()),
            new NodeCreated(testData3Info.getId(), testFolderInfo.getId()),
            new NodeConsistent(testDataInfo.getId()),
            new NodeConsistent(testData2Info.getId()),
            new NodeConsistent(testData3Info.getId()));

        // check info are correctly stored even with metadata
        assertEquals(testData2Info, storage.getNodeInfo(testData2Info.getId()));

        // check test folder has 3 children
        assertEquals(3, storage.getChildNodes(testFolderInfo.getId()).size());

        // check data nodes initial dependency state
        assertTrue(storage.getDependencies(testDataInfo.getId()).isEmpty());
        assertTrue(storage.getDependencies(testData2Info.getId()).isEmpty());
        assertTrue(storage.getBackwardDependencies(testDataInfo.getId()).isEmpty());
        assertTrue(storage.getBackwardDependencies(testData2Info.getId()).isEmpty());
    }

    private void assertNamedDataItemsCreation(NodeInfo testFolderInfo) throws IOException, InterruptedException {
        DataSource ds1 = new AppStorageDataSource(storage, testFolderInfo.getId(), testFolderInfo.getName());
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(testFolderInfo.getId(), "testData1"), StandardCharsets.UTF_8)) {
            writer.write("Content for testData1");

            //Event must not be sent before stream is closed: should still be empty for now
            storage.flush();
            assertEventStack();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(testFolderInfo.getId(), "testData2"), StandardCharsets.UTF_8)) {
            writer.write("Content for testData2");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(testFolderInfo.getId(), "DATA_SOURCE_SUFFIX_EXT__Test3__ext"), StandardCharsets.UTF_8)) {
            writer.write("Content for DATA_SOURCE_SUFFIX_EXT__Test3__ext");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(testFolderInfo.getId(), "DATA_SOURCE_FILE_NAME__Test4"), StandardCharsets.UTF_8)) {
            writer.write("Content for DATA_SOURCE_FILE_NAME__Test4");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        storage.flush();

        // check events
        assertEventStack(new NodeDataUpdated(testFolderInfo.getId(), "testData1"),
            new NodeDataUpdated(testFolderInfo.getId(), "testData2"),
            new NodeDataUpdated(testFolderInfo.getId(), "DATA_SOURCE_SUFFIX_EXT__Test3__ext"),
            new NodeDataUpdated(testFolderInfo.getId(), "DATA_SOURCE_FILE_NAME__Test4"));

        // check data names
        assertEquals(Set.of("testData2", "testData1", "DATA_SOURCE_SUFFIX_EXT__Test3__ext", "DATA_SOURCE_FILE_NAME__Test4"),
            storage.getDataNames(testFolderInfo.getId()));

        // check data names seen from data source
        assertEquals(Set.of("testData2", "testData1"), ds1.listNames("^testD.*"));
        AfsStorageException error = assertThrows(AfsStorageException.class, () -> ds1.listNames("^DATA_SOURCE_SUFFIX.*"));
        assertEquals("Don't know how to unmap suffix-and-extension to a data source name DATA_SOURCE_SUFFIX_EXT__Test3__ext",
            error.getMessage());
        assertEquals(Set.of("Test4"), ds1.listNames("^DATA_SOURCE_FILE.*"));

        // check children names (not data names)
        List<String> expectedChildrenNames = List.of("data", "data2", "data3");
        List<String> actualChildrenNames = storage.getChildNodes(testFolderInfo.getId()).stream()
            .map(NodeInfo::getName).collect(Collectors.toList());
        assertEquals(expectedChildrenNames, actualChildrenNames);
    }

    private void assertFirstDependencyCreation(NodeInfo testDataInfo, NodeInfo testData2Info) throws InterruptedException {
        storage.addDependency(testDataInfo.getId(), "mylink", testData2Info.getId());
        storage.flush();

        // check event
        assertEventStack(new DependencyAdded(testDataInfo.getId(), "mylink"),
            new BackwardDependencyAdded(testData2Info.getId(), "mylink"));

        // check dependency state
        assertEquals(Set.of(new NodeDependency("mylink", testData2Info)), storage.getDependencies(testDataInfo.getId()));
        assertEquals(Set.of(testDataInfo), storage.getBackwardDependencies(testData2Info.getId()));
        assertEquals(Set.of(testData2Info), storage.getDependencies(testDataInfo.getId(), "mylink"));
        assertTrue(storage.getDependencies(testDataInfo.getId(), "mylink2").isEmpty());
    }

    private void assertAddRemoveSecondDependency(NodeInfo testDataInfo, NodeInfo testData2Info) throws InterruptedException {
        storage.addDependency(testDataInfo.getId(), "mylink2", testData2Info.getId());
        storage.flush();

        // check event
        assertEventStack(new DependencyAdded(testDataInfo.getId(), "mylink2"),
            new BackwardDependencyAdded(testData2Info.getId(), "mylink2"));

        assertEquals(Set.of(new NodeDependency("mylink", testData2Info), new NodeDependency("mylink2", testData2Info)), storage.getDependencies(testDataInfo.getId()));
        assertEquals(Set.of(testDataInfo), storage.getBackwardDependencies(testData2Info.getId()));
        assertEquals(Set.of(testData2Info), storage.getDependencies(testDataInfo.getId(), "mylink"));

        storage.removeDependency(testDataInfo.getId(), "mylink2", testData2Info.getId());
        storage.flush();

        // check event
        assertEventStack(new DependencyRemoved(testDataInfo.getId(), "mylink2"),
            new BackwardDependencyRemoved(testData2Info.getId(), "mylink2"));

        assertEquals(Set.of(new NodeDependency("mylink", testData2Info)), storage.getDependencies(testDataInfo.getId()));
        assertEquals(Set.of(testDataInfo), storage.getBackwardDependencies(testData2Info.getId()));
        assertEquals(Set.of(testData2Info), storage.getDependencies(testDataInfo.getId(), "mylink"));
    }

    private void assertDataNodeDeletion(NodeInfo testFolderInfo, NodeInfo testDataInfo, NodeInfo testData2Info) throws InterruptedException {
        assertEquals(testFolderInfo.getId(), storage.deleteNode(testDataInfo.getId()));
        storage.flush();

        // check event
        assertEventStack(
            new DependencyRemoved(testDataInfo.getId(), "mylink"),
            new BackwardDependencyRemoved(testData2Info.getId(), "mylink"),
            new NodeRemoved(testDataInfo.getId(), testFolderInfo.getId()));

        // check test folder children have been correctly updated
        assertEquals(2, storage.getChildNodes(testFolderInfo.getId()).size());

        // check data node 2 backward dependency has been correctly updated
        assertTrue(storage.getBackwardDependencies(testData2Info.getId()).isEmpty());
    }

    private void checkDataNodeMetadataValues(NodeInfo testData2Info) {
        assertEquals(Map.of("s1", "v1"), testData2Info.getGenericMetadata().getStrings());
        assertEquals(Map.of("d1", 1d), testData2Info.getGenericMetadata().getDoubles());
        assertEquals(Map.of("i1", 2), testData2Info.getGenericMetadata().getInts());
        assertEquals(Map.of("b1", false), testData2Info.getGenericMetadata().getBooleans());

        assertEqualsFromParent(testData2Info);
    }

    private void assertBinaryDataWrite(NodeInfo testData2Info) throws IOException, InterruptedException {
        try (OutputStream os = storage.writeBinaryData(testData2Info.getId(), "blob")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        // check event
        assertEventStack(new NodeDataUpdated(testData2Info.getId(), "blob"));

        try (InputStream is = storage.readBinaryData(testData2Info.getId(), "blob").orElseThrow(AssertionError::new)) {
            assertEquals("word2", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));

        }
        assertTrue(storage.dataExists(testData2Info.getId(), "blob"));
        assertFalse(storage.dataExists(testData2Info.getId(), "blob2"));
        assertEquals(Set.of("blob"), storage.getDataNames(testData2Info.getId()));
    }

    private void assertDataRemove(NodeInfo testData2Info) throws InterruptedException {
        assertFalse(storage.removeData(testData2Info.getId(), "blob2"));
        assertTrue(storage.removeData(testData2Info.getId(), "blob"));
        storage.flush();

        // check event
        assertEventStack(new NodeDataRemoved(testData2Info.getId(), "blob"));

        assertTrue(storage.getDataNames(testData2Info.getId()).isEmpty());
        assertFalse(storage.readBinaryData(testData2Info.getId(), "blob").isPresent());
    }

    private void checkDataSourceViaPattern(NodeInfo testData2Info, DataSource ds) throws IOException, InterruptedException {
        assertEquals(testData2Info.getName(), ds.getBaseName());
        assertFalse(ds.exists(null, "ext"));
        try (OutputStream os = ds.newOutputStream(null, "ext", false)) {
            os.write("word1".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        // check event
        assertEventStack(new NodeDataUpdated(testData2Info.getId(), "DATA_SOURCE_SUFFIX_EXT____ext"));

        assertTrue(ds.exists(null, "ext"));
        try (InputStream ignored = ds.newInputStream(null, "ext2")) {
            fail();
        } catch (Exception ignored) {
            // Nothing to do here
        }
        try (InputStream is = ds.newInputStream(null, "ext")) {
            assertEquals("word1", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
        }

        assertFalse(ds.exists("file1"));
    }

    private void checkDataSourceViaFileName(NodeInfo testData2Info, DataSource ds) throws IOException, InterruptedException {
        try (OutputStream os = ds.newOutputStream("file1", false)) {
            os.write("word1".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        // check event
        assertEventStack(new NodeDataUpdated(testData2Info.getId(), "DATA_SOURCE_FILE_NAME__file1"));

        assertTrue(ds.exists("file1"));
        try (InputStream ignored = ds.newInputStream("file2")) {
            fail();
        } catch (Exception ignored) {
            // Nothing to do here
        }
        try (InputStream is = ds.newInputStream("file1")) {
            assertEquals("word1", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
        }
    }

    private void assertDoubleTimeSeriesCreation(NodeInfo testData2Info, NodeInfo testData3Info) throws InterruptedException {
        TimeSeriesMetadata metadata1 = new TimeSeriesMetadata("ts1",
            TimeSeriesDataType.DOUBLE,
            Map.of("var1", "value1"),
            RegularTimeSeriesIndex.create(Interval.parse("2015-01-01T00:00:00Z/2015-01-01T01:15:00Z"),
                Duration.ofMinutes(15)));
        storage.createTimeSeries(testData2Info.getId(), metadata1);
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesCreated(testData2Info.getId(), "ts1"));

        // check double time series query
        assertEquals(Sets.newHashSet("ts1"), storage.getTimeSeriesNames(testData2Info.getId()));
        assertTrue(storage.timeSeriesExists(testData2Info.getId(), "ts1"));
        assertFalse(storage.timeSeriesExists(testData2Info.getId(), "ts9"));
        assertFalse(storage.timeSeriesExists(testData3Info.getId(), "ts1"));
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(testData2Info.getId(), Sets.newHashSet("ts1"));
        assertEquals(1, metadataList.size());
        assertEquals(metadata1, metadataList.get(0));
        assertTrue(storage.getTimeSeriesMetadata(testData3Info.getId(), Sets.newHashSet("ts1")).isEmpty());
    }

    private void assertAddDataToDoubleTimeSeries(NodeInfo testData2Info, NodeInfo testData3Info) throws InterruptedException {
        storage.addDoubleTimeSeriesData(testData2Info.getId(), 0, "ts1",
            List.of(new CompressedDoubleDataChunk(2, 2, new double[] {1d, 2d}, new int[] {1, 1}),
                new UncompressedDoubleDataChunk(5, new double[] {3d})));
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesDataUpdated(testData2Info.getId(), "ts1"));

        // check versions
        assertEquals(Set.of(0), storage.getTimeSeriesDataVersions(testData2Info.getId()));
        assertEquals(Set.of(0), storage.getTimeSeriesDataVersions(testData2Info.getId(), "ts1"));

        // check double time series data query
        Map<String, List<DoubleDataChunk>> doubleTimeSeriesData = storage.getDoubleTimeSeriesData(testData2Info.getId(), Sets.newHashSet("ts1"), 0);
        assertEquals(1, doubleTimeSeriesData.size());
        assertEquals(List.of(new CompressedDoubleDataChunk(2, 2, new double[] {1d, 2d}, new int[] {1, 1}),
                new UncompressedDoubleDataChunk(5, new double[] {3d})),
            doubleTimeSeriesData.get("ts1"));
        assertTrue(storage.getDoubleTimeSeriesData(testData3Info.getId(), Sets.newHashSet("ts1"), 0).isEmpty());
    }

    private void assertStringTimeSeriesCreation(NodeInfo testData2Info) throws InterruptedException {
        TimeSeriesMetadata metadata2 = new TimeSeriesMetadata("ts2",
            TimeSeriesDataType.STRING,
            Map.of(),
            RegularTimeSeriesIndex.create(Interval.parse("2015-01-01T00:00:00Z/2015-01-01T01:15:00Z"),
                Duration.ofMinutes(15)));
        storage.createTimeSeries(testData2Info.getId(), metadata2);
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesCreated(testData2Info.getId(), "ts2"));

        // check string time series query
        assertEquals(Sets.newHashSet("ts1", "ts2"), storage.getTimeSeriesNames(testData2Info.getId()));
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(testData2Info.getId(), Sets.newHashSet("ts1"));
        assertEquals(1, metadataList.size());
    }

    private void assertAddDataToStringTimeSeries(NodeInfo testData2Info) throws InterruptedException {
        storage.addStringTimeSeriesData(testData2Info.getId(), 0, "ts2", List.of(new CompressedStringDataChunk(2, 2, new String[] {"a", "b"}, new int[] {1, 1}),
            new UncompressedStringDataChunk(5, new String[] {"c"})));
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesDataUpdated(testData2Info.getId(), "ts2"));

        // check string time series data query
        Map<String, List<StringDataChunk>> stringTimeSeriesData = storage.getStringTimeSeriesData(testData2Info.getId(), Sets.newHashSet("ts2"), 0);
        assertEquals(1, stringTimeSeriesData.size());
        assertEquals(List.of(new CompressedStringDataChunk(2, 2, new String[] {"a", "b"}, new int[] {1, 1}),
                new UncompressedStringDataChunk(5, new String[] {"c"})),
            stringTimeSeriesData.get("ts2"));
    }

    private void assertClearTimeSeries(NodeInfo testData2Info) throws InterruptedException {
        storage.clearTimeSeries(testData2Info.getId());
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesCleared(testData2Info.getId()));

        // check there is no more time series
        assertTrue(storage.getTimeSeriesNames(testData2Info.getId()).isEmpty());
    }

    private void assertChangeParent(NodeInfo rootFolderInfo) throws InterruptedException {
        NodeInfo folder1Info = storage.createNode(rootFolderInfo.getId(), "test1", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo folder2Info = storage.createNode(rootFolderInfo.getId(), "test2", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder1Info.getId());
        storage.setConsistent(folder2Info.getId());
        storage.flush();

        discardEvents(4);

        // create a file in folder 1
        NodeInfo fileInfo = storage.createNode(folder1Info.getId(), "file", "file-type", "", 0, new NodeGenericMetadata());
        storage.setConsistent(fileInfo.getId());
        storage.flush();

        discardEvents(2);

        // check parent folder
        assertEquals(folder1Info, storage.getParentNode(fileInfo.getId()).orElseThrow(AssertionError::new));

        // change parent to folder 2
        storage.setParentNode(fileInfo.getId(), folder2Info.getId());
        storage.flush();

        // check event
        assertEventStack(new ParentChanged(fileInfo.getId(), folder1Info.getId(), folder2Info.getId()));

        // check parent folder change
        assertEquals(folder2Info, storage.getParentNode(fileInfo.getId()).orElseThrow(AssertionError::new));
    }

    private void assertDeleteNode(NodeInfo rootFolderInfo) throws InterruptedException {
        // create root node and 2 folders
        NodeInfo folder3Info = storage.createNode(rootFolderInfo.getId(), "test3", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo folder4Info = storage.createNode(rootFolderInfo.getId(), "test4", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder3Info.getId());
        storage.setConsistent(folder4Info.getId());
        storage.flush();

        discardEvents(2);

        storage.addDependency(folder3Info.getId(), "dep", folder4Info.getId());
        storage.addDependency(folder3Info.getId(), "dep2", folder4Info.getId());
        storage.flush();

        discardEvents(2);

        assertEquals(Collections.singleton(folder4Info), storage.getDependencies(folder3Info.getId(), "dep"));
        assertEquals(Collections.singleton(folder4Info), storage.getDependencies(folder3Info.getId(), "dep2"));

        storage.deleteNode(folder4Info.getId());

        assertTrue(storage.getDependencies(folder3Info.getId(), "dep").isEmpty());
        assertTrue(storage.getDependencies(folder3Info.getId(), "dep2").isEmpty());
    }

    private void assertRenameNode(NodeInfo rootFolderInfo) {
        NodeGenericMetadata folder5Metadata = new NodeGenericMetadata().setString("k", "v");
        NodeInfo folder5Info = storage.createNode(rootFolderInfo.getId(), "test5", FOLDER_PSEUDO_CLASS, "", 0, folder5Metadata);
        storage.setConsistent(folder5Info.getId());
        NodeInfo folder51Info = storage.createNode(folder5Info.getId(), "child_of_test5", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo folder52Info = storage.createNode(folder5Info.getId(), "another_child_of_test5", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder51Info.getId());
        storage.setConsistent(folder52Info.getId());
        storage.flush();

        String newName = "newtest5";

        storage.renameNode(folder5Info.getId(), newName);
        storage.flush();
        folder5Info = storage.getNodeInfo(folder5Info.getId());
        assertEquals(newName, folder5Info.getName());
        assertEquals(2, storage.getChildNodes(folder5Info.getId()).size());
        assertTrue(storage.getChildNode(folder5Info.getId(), "child_of_test5").isPresent());
        assertTrue(storage.getChildNode(folder5Info.getId(), "another_child_of_test5").isPresent());
        assertEquals("v", folder5Info.getGenericMetadata().getString("k"));
        assertEqualsFromParent(folder5Info);

        NodeInfo folder6Info = storage.createNode(rootFolderInfo.getId(), "test6", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder6Info.getId());
        try {
            storage.renameNode(folder6Info.getId(), null);
            fail();
        } catch (Exception ignored) {
            // Nothing to do here
        }

        NodeInfo folder7Info = storage.createNode(rootFolderInfo.getId(), "test7", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder7Info.getId());
        try {
            storage.renameNode(folder7Info.getId(), "");
            fail();
        } catch (Exception ignored) {
            // Nothing to do here
        }
    }

    private void checkEventBus() throws InterruptedException {
        assertNotNull(storage.getEventsBus());

        storage.flush();

        String topic = "some topic";
        CountDownLatch eventReceived = new CountDownLatch(1);
        AtomicReference<NodeEventList> eventsCatched = new AtomicReference<>();
        storage.getEventsBus().addListener(new AppStorageListener() {
            @Override
            public void onEvents(NodeEventList eventList) {
                eventsCatched.set(eventList);
                eventReceived.countDown();
            }

            @Override
            public Set<String> topics() {
                return Collections.singleton(topic);
            }
        });

        NodeEvent eventToCatch = new NodeCreated("test2", "test");
        NodeEvent eventNotToCatch = new NodeCreated("test1", "test");

        storage.getEventsBus().pushEvent(eventNotToCatch, "other topic");
        storage.getEventsBus().pushEvent(eventToCatch, topic);
        storage.flush();

        eventReceived.await(1000, TimeUnit.MILLISECONDS);
        assertThat(eventsCatched.get()).isNotNull();
        assertThat(eventsCatched.get().getTopic()).isEqualTo(topic);
        assertThat(eventsCatched.get().getEvents()).hasSize(1);
        assertThat(eventsCatched.get().getEvents().get(0)).isEqualTo(eventToCatch);
        assertEventStack(eventNotToCatch, eventToCatch);
    }

    @Test
    public void test() throws IOException, InterruptedException {
        // 1) create root folder
        NodeInfo rootFolderInfo = assertRootFolderCreation();

        // 2) create a test folder
        NodeInfo testFolderInfo = assertTestFolderCreation(rootFolderInfo);

        // 3) check description initial value and update
        assertDescription(testFolderInfo);

        // 4) check modifiation time update
        assertModificationTimeUpdate(testFolderInfo);

        // 5) create 2 data nodes in test folder
        NodeInfo testDataInfo = storage.createNode(testFolderInfo.getId(), "data", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo testData2Info = storage.createNode(testFolderInfo.getId(), "data2", DATA_FILE_CLASS, "", 0,
            new NodeGenericMetadata().setString("s1", "v1")
                .setDouble("d1", 1d)
                .setInt("i1", 2)
                .setBoolean("b1", false));
        NodeInfo testData3Info = storage.createNode(testFolderInfo.getId(), "data3", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(testDataInfo.getId());
        storage.setConsistent(testData2Info.getId());
        storage.setConsistent(testData3Info.getId());
        storage.flush();
        assertDataNodesCreation(testFolderInfo, testDataInfo, testData2Info, testData3Info);

        // 5b) create named data items in test folder
        assertNamedDataItemsCreation(testFolderInfo);

        // 6) create a dependency between data node and data node 2
        assertFirstDependencyCreation(testDataInfo, testData2Info);

        // 7) add then add a second dependency
        assertAddRemoveSecondDependency(testDataInfo, testData2Info);

        // 8) delete data node
        assertDataNodeDeletion(testFolderInfo, testDataInfo, testData2Info);

        // 9) check data node 2 metadata value
        checkDataNodeMetadataValues(testData2Info);

        // 10) check data node 2 binary data write
        assertBinaryDataWrite(testData2Info);

        // 10 bis) check data remove
        assertDataRemove(testData2Info);

        // 11) check data source using pattern api
        DataSource ds = new AppStorageDataSource(storage, testData2Info.getId(), testData2Info.getName());
        checkDataSourceViaPattern(testData2Info, ds);

        // 12) check data source using file name api
        checkDataSourceViaFileName(testData2Info, ds);

        // 13) create double time series
        assertDoubleTimeSeriesCreation(testData2Info, testData3Info);

        // 14) add data to double time series
        assertAddDataToDoubleTimeSeries(testData2Info, testData3Info);

        // 15) create a second string time series
        assertStringTimeSeriesCreation(testData2Info);

        // 16) add data to double time series
        assertAddDataToStringTimeSeries(testData2Info);

        // 17) clear time series
        assertClearTimeSeries(testData2Info);

        // 18) change parent test
        assertChangeParent(rootFolderInfo);

        // 18) delete node test
        assertDeleteNode(rootFolderInfo);

        // 19) rename node test
        assertRenameNode(rootFolderInfo);

        // 20) Test update node metadata
        testUpdateNodeMetadata(rootFolderInfo, storage);

        // 21) check that eventsBus is not null
        checkEventBus();

        testCascadingDelete(rootFolderInfo);

        nextDependentTests();
    }

    /**
     * Checks consistency between this node info, and the same node info retrieved from the parent.
     */
    private void assertEqualsFromParent(NodeInfo node) {
        NodeInfo parent = storage.getParentNode(node.getId())
            .orElseThrow(AssertionError::new);
        NodeInfo nodeFromParent = storage.getChildNode(parent.getId(), node.getName())
            .orElseThrow(AssertionError::new);
        assertEquals(node, nodeFromParent);
    }

    protected void nextDependentTests() throws InterruptedException {
        // Noop
        // allow sub classes to continue tests using created node root
    }

    protected void testUpdateNodeMetadata(NodeInfo rootFolderInfo, AppStorage storage) throws InterruptedException {
        NodeGenericMetadata metadata = new NodeGenericMetadata();
        NodeInfo node = storage.createNode(rootFolderInfo.getId(), "testNode", "unknownFile", "", 0, cloneMetadata(metadata));
        storage.setConsistent(node.getId());

        storage.flush();

        checkMetadataEquality(metadata, node.getGenericMetadata());

        discardEvents(22);

        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();
        assertEventStack(new NodeMetadataUpdated(node.getId(), metadata));

        metadata.setString("test", "test");
        assertThat(node.getGenericMetadata().getStrings().keySet()).isEmpty();

        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();
        assertEventStack(new NodeMetadataUpdated(node.getId(), metadata));
        node = storage.getNodeInfo(node.getId());
        checkMetadataEquality(metadata, node.getGenericMetadata());
        node = storage.getChildNode(rootFolderInfo.getId(), "testNode").get();
        checkMetadataEquality(metadata, node.getGenericMetadata());

        metadata.setBoolean("test1", true);
        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();
        assertEventStack(new NodeMetadataUpdated(node.getId(), metadata));
        node = storage.getNodeInfo(node.getId());
        checkMetadataEquality(metadata, node.getGenericMetadata());
        node = storage.getChildNode(rootFolderInfo.getId(), "testNode").get();
        checkMetadataEquality(metadata, node.getGenericMetadata());

        metadata.getStrings().remove("test");
        metadata.setDouble("test2", 0.1);
        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();
        assertEventStack(new NodeMetadataUpdated(node.getId(), metadata));
        node = storage.getNodeInfo(node.getId());
        checkMetadataEquality(metadata, node.getGenericMetadata());
        node = storage.getChildNode(rootFolderInfo.getId(), "testNode").get();
        checkMetadataEquality(metadata, node.getGenericMetadata());

        metadata.setInt("test3", 1);
        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();
        assertEventStack(new NodeMetadataUpdated(node.getId(), metadata));
        node = storage.getNodeInfo(node.getId());
        checkMetadataEquality(metadata, node.getGenericMetadata());
        node = storage.getChildNode(rootFolderInfo.getId(), "testNode").get();
        checkMetadataEquality(metadata, node.getGenericMetadata());

        storage.deleteNode(node.getId());
        storage.flush();
        // clean delete node event
        eventStack.take();
    }

    private void checkMetadataEquality(NodeGenericMetadata source, NodeGenericMetadata target) {
        assertThat(target).isNotNull();
        assertEquals(target.getBooleans().keySet().size(), source.getBooleans().keySet().size());
        source.getBooleans().forEach((key, val) -> {
            assertThat(target.getBooleans()).contains(new HashMap.SimpleEntry<>(key, val));
        });
        assertEquals(target.getStrings().keySet().size(), source.getStrings().keySet().size());
        source.getStrings().forEach((key, val) -> {
            assertThat(target.getStrings()).contains(new HashMap.SimpleEntry<>(key, val));
        });
        assertEquals(target.getInts().keySet().size(), source.getInts().keySet().size());
        source.getInts().forEach((key, val) -> {
            assertThat(target.getInts()).contains(new HashMap.SimpleEntry<>(key, val));
        });
        assertEquals(target.getDoubles().keySet().size(), source.getDoubles().keySet().size());
        source.getDoubles().forEach((key, val) -> {
            assertThat(target.getDoubles()).contains(new HashMap.SimpleEntry<>(key, val));
        });
    }

    private NodeGenericMetadata cloneMetadata(NodeGenericMetadata metadata) {
        NodeGenericMetadata clone = new NodeGenericMetadata();
        clone.getStrings().putAll(metadata.getStrings());
        clone.getBooleans().putAll(metadata.getBooleans());
        clone.getInts().putAll(metadata.getInts());
        clone.getDoubles().putAll(metadata.getDoubles());
        return clone;
    }

    private void testCascadingDelete(NodeInfo rootFolderInfo) throws IOException, InterruptedException {
        NodeInfo subFolder = storage.createNode(rootFolderInfo.getId(), "test-delete", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(subFolder.getId());
        NodeInfo subSubFolder = storage.createNode(subFolder.getId(), "sub-folder", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(subSubFolder.getId());
        NodeInfo data1 = storage.createNode(subFolder.getId(), "data1", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(data1.getId());
        NodeInfo data2 = storage.createNode(subSubFolder.getId(), "data1", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(data2.getId());

        try (OutputStream outputStream = storage.writeBinaryData(data1.getId(), "data1")) {
            outputStream.write("data1".getBytes(StandardCharsets.UTF_8));
        }
        try (OutputStream outputStream = storage.writeBinaryData(data2.getId(), "data2")) {
            outputStream.write("data2".getBytes(StandardCharsets.UTF_8));
        }

        storage.deleteNode(subFolder.getId());
        storage.flush();

        //Dicard events up until deletion
        discardEvents(10);

        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> storage.getNodeInfo(subFolder.getId()));
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> storage.getNodeInfo(subSubFolder.getId()));
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> storage.getNodeInfo(data1.getId()));
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> storage.getNodeInfo(data2.getId()));

        assertEventStackInAnyOrder(
            new NodeRemoved(subFolder.getId(), rootFolderInfo.getId()),
            new NodeRemoved(subSubFolder.getId(), subFolder.getId()),
            new NodeRemoved(data1.getId(), subFolder.getId()),
            new NodeRemoved(data2.getId(), subSubFolder.getId()),
            new NodeDataRemoved(data1.getId(), "data1"),
            new NodeDataRemoved(data2.getId(), "data2")
        );
    }
}
