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
import com.powsybl.timeseries.CompressedDoubleDataChunk;
import com.powsybl.timeseries.CompressedStringDataChunk;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.RegularTimeSeriesIndex;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesDataType;
import com.powsybl.timeseries.TimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesMetadata;
import com.powsybl.timeseries.UncompressedDoubleDataChunk;
import com.powsybl.timeseries.UncompressedStringDataChunk;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.threeten.extra.Interval;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Abstract class used to test the multiple implementations of AppStorage
 * <p>
 *     In multiple places, {@code await().until(() -> !eventStack.isEmpty());} is used in order to wait for the event
 *     stack to be updated (especially useful for implementations using remote servers
 * </p>
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class AbstractAppStorageTest {

    protected static final String FOLDER_PSEUDO_CLASS = "folder";
    static final String DATA_FILE_CLASS = "data";
    private static final String NODE_NOT_FOUND_REGEX = "Node [0-9a-fA-F-]{36} not found";
    private static final Pattern NODE_NOT_FOUND_PATTERN = Pattern.compile(NODE_NOT_FOUND_REGEX);
    private static final String IMPOSSIBLE_TO_RENAME_NODE_REGEX = "Impossible to rename node '[0-9a-fA-F-]{36}' with an empty name";
    private static final Pattern IMPOSSIBLE_TO_RENAME_NODE_PATTERN = Pattern.compile(IMPOSSIBLE_TO_RENAME_NODE_REGEX);

    protected AppStorage storage;

    protected BlockingQueue<NodeEvent> eventStack;

    protected AppStorageListener l = eventList -> eventStack.addAll(eventList.getEvents());

    private NodeInfo rootFolderInfo;
    private NodeInfo testFolderInfo;
    private List<NodeInfo> dataNodeInfos;

    protected abstract AppStorage createStorage();

    @BeforeEach
    public void setUp() {
        eventStack = new LinkedBlockingQueue<>();
        this.storage = createStorage();
        this.storage.getEventsBus().addListener(l);
    }

    @AfterEach
    public void tearDown() {
        if (!storage.isClosed()) {
            storage.close();
        }
        clearEventStack();
    }

    public void clearAllNodes() {
        if (testFolderInfo != null) {
            storage.deleteNode(testFolderInfo.getId());
            List<NodeInfo> childNodesInfo = storage.getChildNodes(rootFolderInfo.getId());
            childNodesInfo.forEach(nodeInfo -> storage.deleteNode(nodeInfo.getId()));
        }
    }

    public void waitIfNeededForEventStack() {
        // Empty by default but you may consider using waitForEventStack here
    }

    public void waitForEventStack() {
        await().until(() -> !eventStack.isEmpty());
    }

    @Test
    @Order(1)
    public void createRootFolderTest() throws InterruptedException {
        // create root folder
        createRootFolder();
        assertNotNull(rootFolderInfo);

        // Assert node consistency
        assertTrue(storage.isConsistent(rootFolderInfo.getId()));

        // check event
        assertEventStack(new NodeCreated(rootFolderInfo.getId(), null),
            new NodeConsistent(rootFolderInfo.getId()));

        // assert root folder is writable
        assertTrue(storage.isWritable(rootFolderInfo.getId()));

        // assert root folder parent is null
        assertFalse(storage.getParentNode(rootFolderInfo.getId()).isPresent());

        // check root folder name and pseudo class is correct
        assertEquals(storage.getFileSystemName(), storage.getNodeInfo(rootFolderInfo.getId()).getName());
        assertEquals(FOLDER_PSEUDO_CLASS, storage.getNodeInfo(rootFolderInfo.getId()).getPseudoClass());

        // assert root folder is empty
        assertTrue(storage.getChildNodes(rootFolderInfo.getId()).isEmpty());
    }

    @Test
    @Order(2)
    public void createTestFolderTest() {
        // Create a test folder
        createTestFolder();

        // By default, the node should not be consistent
        assertFalse(storage.isConsistent(testFolderInfo.getId()));
        assertEquals(1, storage.getInconsistentNodes().size());
        assertEquals(testFolderInfo.getId(), storage.getInconsistentNodes().get(0).getId());
    }

    @Test
    @Order(3)
    public void createConsistentTestFolderTest() throws InterruptedException {
        // Create a test folder
        createConsistentTestFolder();

        // The node should now be consistent
        assertTrue(storage.isConsistent(testFolderInfo.getId()));
        assertTrue(storage.getInconsistentNodes().isEmpty());

        // Check events
        assertEventStack(new NodeCreated(testFolderInfo.getId(), rootFolderInfo.getId()),
            new NodeConsistent(testFolderInfo.getId()));
    }

    @Test
    @Order(4)
    public void getParentOnTestFolderTest() {
        // Create a test folder
        createConsistentTestFolder();

        // assert parent of test folder is root folder
        assertEquals(rootFolderInfo, storage.getParentNode(testFolderInfo.getId()).orElseThrow(AssertionError::new));
    }

    @Test
    @Order(5)
    public void getNodeInfoOnTestFolderTest() {
        // Create a test folder
        createConsistentTestFolder();

        // Retrieve the node info
        NodeInfo retrievedTestFolderInfo = storage.getNodeInfo(testFolderInfo.getId());

        // Checks on the node info
        assertEquals(testFolderInfo.getId(), retrievedTestFolderInfo.getId());
        assertEquals("test", retrievedTestFolderInfo.getName());
        assertEquals(FOLDER_PSEUDO_CLASS, retrievedTestFolderInfo.getPseudoClass());
        assertEquals(12, retrievedTestFolderInfo.getVersion());
        assertEquals("", retrievedTestFolderInfo.getDescription());
        assertTrue(retrievedTestFolderInfo.getCreationTime() > 0);
        assertTrue(retrievedTestFolderInfo.getModificationTime() > 0);

        // Checks on the metadata
        assertEquals(Collections.singletonMap("k", "v"), retrievedTestFolderInfo.getGenericMetadata().getStrings());
        assertTrue(retrievedTestFolderInfo.getGenericMetadata().getDoubles().isEmpty());
        assertTrue(retrievedTestFolderInfo.getGenericMetadata().getInts().isEmpty());
        assertTrue(retrievedTestFolderInfo.getGenericMetadata().getBooleans().isEmpty());
    }

    @Test
    @Order(6)
    public void justCreatedTestFolderIsEmptyTest() {
        // Create a test folder
        createConsistentTestFolder();

        // Check test folder is empty
        assertTrue(storage.getChildNodes(testFolderInfo.getId()).isEmpty());
    }

    @Test
    @Order(7)
    public void getChildNodesOnRootFolderAfterTestFolderCreationTest() {
        // Create a test folder
        createConsistentTestFolder();

        // check root folder has one child (test folder)
        assertEquals(1, storage.getChildNodes(rootFolderInfo.getId()).size());
        assertEquals(testFolderInfo, storage.getChildNodes(rootFolderInfo.getId()).get(0));
        assertTrue(storage.getChildNode(rootFolderInfo.getId(), "test").isPresent());
        assertEquals(testFolderInfo, storage.getChildNode(rootFolderInfo.getId(), "test").orElseThrow(AssertionError::new));

        // check getChildNode return null if child does not exist
        assertFalse(storage.getChildNode(rootFolderInfo.getId(), "???").isPresent());
    }

    @Test
    @Order(8)
    public void setDescriptionOnTestFolderTest() throws InterruptedException {
        // Set the test folder description
        setTestFolderDescription();

        // Check the description
        assertEquals("hello", storage.getNodeInfo(testFolderInfo.getId()).getDescription());

        // check event
        assertEventStack(new NodeDescriptionUpdated(testFolderInfo.getId(), "hello"));
    }

    @Test
    @Order(9)
    public void updateModificationTimeOnTestFolderTest() {
        // Previous things to do
        setTestFolderDescription();

        // Get current modification time
        long oldModificationTime = testFolderInfo.getModificationTime();

        // Update the date
        updateModificationTimeOnTestFolder();

        // Compare the new date
        assertTrue(storage.getNodeInfo(testFolderInfo.getId()).getModificationTime() >= oldModificationTime);
    }

    @Test
    @Order(10)
    public void createDataNodesTest() throws InterruptedException {
        // Create the 3 data nodes
        createDataNodes();

        // Check events
        assertEventStack(new NodeCreated(dataNodeInfos.get(0).getId(), testFolderInfo.getId()),
            new NodeCreated(dataNodeInfos.get(1).getId(), testFolderInfo.getId()),
            new NodeCreated(dataNodeInfos.get(2).getId(), testFolderInfo.getId()),
            new NodeConsistent(dataNodeInfos.get(0).getId()),
            new NodeConsistent(dataNodeInfos.get(1).getId()),
            new NodeConsistent(dataNodeInfos.get(2).getId()));

        // Check info are correctly stored even with metadata
        assertEquals(dataNodeInfos.get(1), storage.getNodeInfo(dataNodeInfos.get(1).getId()));

        // Check test folder has 3 children
        assertEquals(3, storage.getChildNodes(testFolderInfo.getId()).size());

        // Check children names
        List<String> expectedChildrenNames = List.of("data", "data2", "data3");
        List<String> actualChildrenNames = storage.getChildNodes(testFolderInfo.getId()).stream()
            .map(NodeInfo::getName).collect(Collectors.toList());
        assertEquals(expectedChildrenNames, actualChildrenNames);

        // Check data nodes initial dependency state
        assertTrue(storage.getDependencies(dataNodeInfos.get(0).getId()).isEmpty());
        assertTrue(storage.getDependencies(dataNodeInfos.get(1).getId()).isEmpty());
        assertTrue(storage.getBackwardDependencies(dataNodeInfos.get(0).getId()).isEmpty());
        assertTrue(storage.getBackwardDependencies(dataNodeInfos.get(1).getId()).isEmpty());
    }

    @Test
    @Order(11)
    public void noEventSentBeforeWriterStreamIsClosedTest() throws IOException, InterruptedException {
        // Create the 3 data nodes
        createDataNodes();
        clearEventStack();

        // Check that no event is sent before the stream is closed
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(testFolderInfo.getId(), "testData1"), StandardCharsets.UTF_8)) {
            writer.write("Content for testData1");
            storage.flush();

            //Event must not be sent before stream is closed: should still be empty for now
            assertEventStack();
        }
    }

    @Test
    @Order(12)
    public void namedDataItemsCreationTest() throws InterruptedException {
        // Write the data
        writeBinaryDataInNodes();

        // check events
        assertEventStack(new NodeDataUpdated(testFolderInfo.getId(), "testData1"),
            new NodeDataUpdated(testFolderInfo.getId(), "testData2"),
            new NodeDataUpdated(testFolderInfo.getId(), "DATA_SOURCE_SUFFIX_EXT__Test3__ext"),
            new NodeDataUpdated(testFolderInfo.getId(), "DATA_SOURCE_FILE_NAME__Test4"));

        // check data names
        assertEquals(Set.of("testData2", "testData1", "DATA_SOURCE_SUFFIX_EXT__Test3__ext", "DATA_SOURCE_FILE_NAME__Test4"),
            storage.getDataNames(testFolderInfo.getId()));
    }

    @Test
    @Order(13)
    public void namedDataItemsCreationSeenFromDataSourceTest() throws IOException {
        // Write the data
        writeBinaryDataInNodes();

        // Create the datasource
        DataSource ds1 = new AppStorageDataSource(storage, testFolderInfo.getId(), testFolderInfo.getName());

        // check data names seen from data source
        assertEquals(Set.of("testData2", "testData1"), ds1.listNames("^testD.*"));
        AssertionError error = assertThrows(AssertionError.class, () -> ds1.listNames("^DATA_SOURCE_SUFFIX.*"));
        assertEquals("Don't know how to unmap suffix-and-extension to a data source name DATA_SOURCE_SUFFIX_EXT__Test3__ext",
            error.getMessage());
        assertEquals(Set.of("Test4"), ds1.listNames("^DATA_SOURCE_FILE.*"));
    }

    @Test
    @Order(14)
    public void firstDependencyCreationTest() throws InterruptedException {
        // Create the first dependency
        createFirstDependency();

        // check event
        assertEventStack(new DependencyAdded(dataNodeInfos.get(0).getId(), "mylink"),
            new BackwardDependencyAdded(dataNodeInfos.get(1).getId(), "mylink"));

        // check dependency state
        assertEquals(Set.of(new NodeDependency("mylink", dataNodeInfos.get(1))), storage.getDependencies(dataNodeInfos.get(0).getId()));
        assertEquals(Set.of(dataNodeInfos.get(0)), storage.getBackwardDependencies(dataNodeInfos.get(1).getId()));
        assertEquals(Set.of(dataNodeInfos.get(1)), storage.getDependencies(dataNodeInfos.get(0).getId(), "mylink"));
        assertTrue(storage.getDependencies(dataNodeInfos.get(0).getId(), "mylink2").isEmpty());
    }

    @Test
    @Order(15)
    public void secondDependencyCreationTest() throws InterruptedException {
        // Create the first dependency
        createFirstDependency();
        waitIfNeededForEventStack();
        clearEventStack();

        // Add the second dependency
        storage.addDependency(dataNodeInfos.get(0).getId(), "mylink2", dataNodeInfos.get(1).getId());
        storage.flush();

        // Check event
        assertEventStack(new DependencyAdded(dataNodeInfos.get(0).getId(), "mylink2"),
            new BackwardDependencyAdded(dataNodeInfos.get(1).getId(), "mylink2"));

        // Check the dependencies
        assertEquals(Set.of(new NodeDependency("mylink", dataNodeInfos.get(1)), new NodeDependency("mylink2", dataNodeInfos.get(1))), storage.getDependencies(dataNodeInfos.get(0).getId()));
        assertEquals(Set.of(dataNodeInfos.get(0)), storage.getBackwardDependencies(dataNodeInfos.get(1).getId()));
        assertEquals(Set.of(dataNodeInfos.get(1)), storage.getDependencies(dataNodeInfos.get(0).getId(), "mylink"));
    }

    @Test
    @Order(16)
    public void secondDependencyRemovalTest() throws InterruptedException {
        // Create the first dependency
        createFirstDependency();
        waitIfNeededForEventStack();

        // Add the second dependency
        storage.addDependency(dataNodeInfos.get(0).getId(), "mylink2", dataNodeInfos.get(1).getId());
        storage.flush();
        clearEventStack();

        // Remove the second dependency
        storage.removeDependency(dataNodeInfos.get(0).getId(), "mylink2", dataNodeInfos.get(1).getId());
        storage.flush();

        // check event
        assertEventStack(new DependencyRemoved(dataNodeInfos.get(0).getId(), "mylink2"),
            new BackwardDependencyRemoved(dataNodeInfos.get(1).getId(), "mylink2"));

        assertEquals(Set.of(new NodeDependency("mylink", dataNodeInfos.get(1))), storage.getDependencies(dataNodeInfos.get(0).getId()));
        assertEquals(Set.of(dataNodeInfos.get(0)), storage.getBackwardDependencies(dataNodeInfos.get(1).getId()));
        assertEquals(Set.of(dataNodeInfos.get(1)), storage.getDependencies(dataNodeInfos.get(0).getId(), "mylink"));
    }

    @Test
    @Order(17)
    public void dataNodeDeletionTest() throws InterruptedException {
        // Delete a node
        String deletedNodeId = deleteNode();

        // Check the ID
        assertEquals(testFolderInfo.getId(), deletedNodeId);

        // check event
        assertEventStack(
            new DependencyRemoved(dataNodeInfos.get(0).getId(), "mylink"),
            new BackwardDependencyRemoved(dataNodeInfos.get(1).getId(), "mylink"),
            new NodeRemoved(dataNodeInfos.get(0).getId(), testFolderInfo.getId()));

        // check test folder children have been correctly updated
        assertEquals(2, storage.getChildNodes(testFolderInfo.getId()).size());

        // check data node 2 backward dependency has been correctly updated
        assertTrue(storage.getBackwardDependencies(dataNodeInfos.get(1).getId()).isEmpty());
    }

    @Test
    @Order(18)
    public void dataNodeMetadataValuesTest() {
        // Set up
        deleteNode();

        // Check the metadata values
        assertEquals(Map.of("s1", "v1"), dataNodeInfos.get(1).getGenericMetadata().getStrings());
        assertEquals(Map.of("d1", 1d), dataNodeInfos.get(1).getGenericMetadata().getDoubles());
        assertEquals(Map.of("i1", 2), dataNodeInfos.get(1).getGenericMetadata().getInts());
        assertEquals(Map.of("b1", false), dataNodeInfos.get(1).getGenericMetadata().getBooleans());
    }

    @Test
    @Order(19)
    public void compareNodeToItselfFromParentTest() {
        // Set up
        deleteNode();

        // Check that the child from the parent of a node is the same node
        assertEqualsFromParent(dataNodeInfos.get(1));
    }

    @Test
    @Order(20)
    public void writeBlobInDataNodeTest() throws IOException, InterruptedException {
        // Write blob in data node
        writeBlobInDataNode2();

        // check event
        assertEventStack(new NodeDataUpdated(dataNodeInfos.get(1).getId(), "blob"));

        // Check blob
        try (InputStream is = storage.readBinaryData(dataNodeInfos.get(1).getId(), "blob").orElseThrow(AssertionError::new)) {
            assertEquals("word2", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));

        }
        assertTrue(storage.dataExists(dataNodeInfos.get(1).getId(), "blob"));
        assertFalse(storage.dataExists(dataNodeInfos.get(1).getId(), "blob2"));
        assertEquals(Set.of("blob"), storage.getDataNames(dataNodeInfos.get(1).getId()));
    }

    @Test
    @Order(21)
    public void removeInexistentBlobTest() throws IOException {
        // Previous things to do
        writeBlobInDataNode2();

        // Try to remove a non-existent blob
        assertFalse(storage.removeData(dataNodeInfos.get(1).getId(), "blob2"));
    }

    @Test
    @Order(22)
    public void removeExistingBlobTest() throws IOException {
        // Previous things to do
        writeBlobInDataNode2();

        // Try to remove a non-existent blob
        assertTrue(storage.removeData(dataNodeInfos.get(1).getId(), "blob"));
    }

    @Test
    @Order(21)
    public void removeBlobInDataNodeTest() throws IOException, InterruptedException {
        // Remove the blob
        removeBlobInDataNode2();

        // check event
        assertEventStack(new NodeDataRemoved(dataNodeInfos.get(1).getId(), "blob"));

        assertTrue(storage.getDataNames(dataNodeInfos.get(1).getId()).isEmpty());
        assertFalse(storage.readBinaryData(dataNodeInfos.get(1).getId(), "blob").isPresent());
    }

    @Test
    @Order(23)
    public void datasourceOnDataNodePatternTest() throws IOException, InterruptedException {
        // Previous things to do
        removeBlobInDataNode2();
        waitIfNeededForEventStack();
        clearEventStack();

        // Create the datasource
        DataSource ds = new AppStorageDataSource(storage, dataNodeInfos.get(1).getId(), dataNodeInfos.get(1).getName());

        // Base name
        assertEquals(dataNodeInfos.get(1).getName(), ds.getBaseName());

        // The file does not yet exist
        assertFalse(ds.exists(null, "ext"));

        // Write a new file
        try (OutputStream os = ds.newOutputStream(null, "ext", false)) {
            os.write("word1".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        // check event
        assertEventStack(new NodeDataUpdated(dataNodeInfos.get(1).getId(), "DATA_SOURCE_SUFFIX_EXT____ext"));

        // The file now exists
        assertTrue(ds.exists(null, "ext"));

        // Read the file
        try (InputStream is = ds.newInputStream(null, "ext")) {
            assertEquals("word1", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
        }
    }

    @Test
    @Order(24)
    public void datasourceByPatternInputStreamOnNonExistingFileExceptionTest() throws IOException {
        // Previous things to do
        removeBlobInDataNode2();

        // Create the datasource
        DataSource ds = new AppStorageDataSource(storage, dataNodeInfos.get(1).getId(), dataNodeInfos.get(1).getName());

        // The datasource cannot read a non-existing file
        try (InputStream ignored = ds.newInputStream(null, "ext2")) {
            fail();
        } catch (IOException exception) {
            assertInstanceOf(IOException.class, exception);
            assertEquals("*.ext2 does not exist", exception.getMessage());
        }
    }

    @Test
    @Order(25)
    public void datasourceOnDataNodeFileNameTest() throws IOException, InterruptedException {
        // Previous things to do
        removeBlobInDataNode2();
        waitIfNeededForEventStack();
        clearEventStack();

        // Create the datasource
        DataSource ds = new AppStorageDataSource(storage, dataNodeInfos.get(1).getId(), dataNodeInfos.get(1).getName());

        // The file does not yet exist
        assertFalse(ds.exists("file1"));

        // Write a new file
        try (OutputStream os = ds.newOutputStream("file1", false)) {
            os.write("word1".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        // check event
        assertEventStack(new NodeDataUpdated(dataNodeInfos.get(1).getId(), "DATA_SOURCE_FILE_NAME__file1"));

        // The file now exists
        assertTrue(ds.exists("file1"));

        // Read the file
        try (InputStream is = ds.newInputStream("file1")) {
            assertEquals("word1", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
        }
    }

    @Test
    @Order(26)
    public void datasourceByFilenameInputStreamOnNonExistingFileExceptionTest() throws IOException {
        // Previous things to do
        removeBlobInDataNode2();

        // Create the datasource
        DataSource ds = new AppStorageDataSource(storage, dataNodeInfos.get(1).getId(), dataNodeInfos.get(1).getName());

        // The datasource cannot read a non-existing file
        try (InputStream ignored = ds.newInputStream("file1")) {
            fail();
        } catch (IOException exception) {
            assertInstanceOf(IOException.class, exception);
            assertEquals("file1 does not exist", exception.getMessage());
        }
    }

    @Test
    @Order(27)
    public void createDoubleTimeSeriesTest() throws IOException, InterruptedException {
        // Create the metadata
        TimeSeriesMetadata metadata = createTimeSeriesMetadata("ts1", TimeSeriesDataType.DOUBLE, Map.of("var1", "value1"));

        // Create the timeseries
        createDoubleTimeSeries(metadata);

        // check event
        assertEventStack(new TimeSeriesCreated(dataNodeInfos.get(1).getId(), "ts1"));

        // check double time series query
        assertEquals(Sets.newHashSet("ts1"), storage.getTimeSeriesNames(dataNodeInfos.get(1).getId()));
        assertTrue(storage.timeSeriesExists(dataNodeInfos.get(1).getId(), "ts1"));
        assertFalse(storage.timeSeriesExists(dataNodeInfos.get(1).getId(), "ts9"));
        assertFalse(storage.timeSeriesExists(dataNodeInfos.get(2).getId(), "ts1"));

        // Check the metadata
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(dataNodeInfos.get(1).getId(), Sets.newHashSet("ts1"));
        assertEquals(1, metadataList.size());
        assertEquals(metadata, metadataList.get(0));
        assertTrue(storage.getTimeSeriesMetadata(dataNodeInfos.get(2).getId(), Sets.newHashSet("ts1")).isEmpty());
    }

    @Test
    @Order(28)
    public void addDataToDoubleTimeSeriesTest() throws InterruptedException, IOException {
        // Add the data to the timeseries
        addDataToDoubleTimeSeries();

        // check event
        assertEventStack(new TimeSeriesDataUpdated(dataNodeInfos.get(1).getId(), "ts1"));

        // check versions
        assertEquals(Set.of(0), storage.getTimeSeriesDataVersions(dataNodeInfos.get(1).getId()));
        assertEquals(Set.of(0), storage.getTimeSeriesDataVersions(dataNodeInfos.get(1).getId(), "ts1"));

        // check double time series data query
        Map<String, List<DoubleDataChunk>> doubleTimeSeriesData = storage.getDoubleTimeSeriesData(dataNodeInfos.get(1).getId(), Sets.newHashSet("ts1"), 0);
        assertEquals(1, doubleTimeSeriesData.size());
        List<DoubleDataChunk> doubleDataChunks = List.of(new CompressedDoubleDataChunk(2, 2, new double[] {1d, 2d}, new int[] {1, 1}),
            new UncompressedDoubleDataChunk(5, new double[] {3d}));
        assertEquals(doubleDataChunks, doubleTimeSeriesData.get("ts1"));

        // The result is empty if the timeseries does not exist
        assertTrue(storage.getDoubleTimeSeriesData(dataNodeInfos.get(2).getId(), Sets.newHashSet("ts1"), 0).isEmpty());
    }

    @Test
    @Order(29)
    public void createStringTimeSeriesTest() throws IOException, InterruptedException {
        // Create the metadata
        TimeSeriesMetadata metadata = createTimeSeriesMetadata("ts2", TimeSeriesDataType.STRING, Map.of());

        // Create the timeseries
        createStringTimeSeries(metadata);

        // check event
        assertEventStack(new TimeSeriesCreated(dataNodeInfos.get(1).getId(), "ts2"));

        // check string time series query
        assertEquals(Sets.newHashSet("ts1", "ts2"), storage.getTimeSeriesNames(dataNodeInfos.get(1).getId()));
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(dataNodeInfos.get(1).getId(), Sets.newHashSet("ts1"));
        assertEquals(1, metadataList.size());
    }

    @Test
    @Order(30)
    public void addDataToStringTimeSeriesTest() throws InterruptedException, IOException {
        // Add data to the timeseries
        addDataToStringTimeSeries();

        // check event
        assertEventStack(new TimeSeriesDataUpdated(dataNodeInfos.get(1).getId(), "ts2"));

        // check string time series data query
        Map<String, List<StringDataChunk>> stringTimeSeriesData = storage.getStringTimeSeriesData(dataNodeInfos.get(1).getId(), Sets.newHashSet("ts2"), 0);
        assertEquals(1, stringTimeSeriesData.size());
        List<StringDataChunk> stringDataChunks = List.of(new CompressedStringDataChunk(2, 2, new String[] {"a", "b"}, new int[] {1, 1}),
            new UncompressedStringDataChunk(5, new String[] {"c"}));
        assertEquals(stringDataChunks, stringTimeSeriesData.get("ts2"));
    }

    @Test
    @Order(31)
    public void clearTimeSeriesTest() throws InterruptedException, IOException {
        // Clear the timeseries
        clearTimeSeries();

        // check event
        assertEventStack(new TimeSeriesCleared(dataNodeInfos.get(1).getId()));

        // check there is no more time series
        assertTrue(storage.getTimeSeriesNames(dataNodeInfos.get(1).getId()).isEmpty());
    }

    @Test
    @Order(32)
    public void changeParentTest() throws InterruptedException, IOException {
        // First create the folders and the file
        createFoldersPlusAFile();

        // Folders
        NodeInfo folder1Info = storage.getChildNode(rootFolderInfo.getId(), "test1").orElseThrow();
        NodeInfo folder2Info = storage.getChildNode(rootFolderInfo.getId(), "test2").orElseThrow();

        // File
        NodeInfo fileInfo = storage.getChildNode(folder1Info.getId(), "file").orElseThrow();

        // check parent folder
        assertEquals(folder1Info, storage.getParentNode(fileInfo.getId()).orElseThrow());

        // Clear the events
        clearEventStack();

        // Change the parent
        changeParent();

        // check event
        assertEventStack(new ParentChanged(fileInfo.getId(), folder1Info.getId(), folder2Info.getId()));

        // check parent folder change
        assertEquals(folder2Info, storage.getParentNode(fileInfo.getId()).orElseThrow());
    }

    @Test
    @Order(33)
    public void deleteNodeTest() throws IOException {
        // Create the nodes
        createNodeForDeletion();
        clearEventStack();

        // Folders
        NodeInfo folder3Info = storage.getChildNode(rootFolderInfo.getId(), "test3").orElseThrow();
        NodeInfo folder4Info = storage.getChildNode(rootFolderInfo.getId(), "test4").orElseThrow();

        // Check that the dependencies exist
        assertEquals(Collections.singleton(folder4Info), storage.getDependencies(folder3Info.getId(), "dep"));
        assertEquals(Collections.singleton(folder4Info), storage.getDependencies(folder3Info.getId(), "dep2"));

        // Delete the node 4
        storage.deleteNode(folder4Info.getId());

        // Check that the dependencies do not exist anymore
        assertTrue(storage.getDependencies(folder3Info.getId(), "dep").isEmpty());
        assertTrue(storage.getDependencies(folder3Info.getId(), "dep2").isEmpty());
    }

    @Test
    @Order(34)
    public void renameNodeTest() throws IOException {
        // Rename a node
        renameNode();

        // Get the node
        NodeInfo folder5Info = storage.getChildNode(rootFolderInfo.getId(), "newtest5").orElseThrow();

        // Some checks on the parent
        assertEquals("newtest5", folder5Info.getName());
        assertEquals(2, storage.getChildNodes(folder5Info.getId()).size());
        assertEquals("v", folder5Info.getGenericMetadata().getString("k"));
        assertEqualsFromParent(folder5Info);

        // Some checks on the children
        assertTrue(storage.getChildNode(folder5Info.getId(), "child_of_test5").isPresent());
        assertTrue(storage.getChildNode(folder5Info.getId(), "another_child_of_test5").isPresent());
    }

    @Test
    @Order(35)
    public void renameNodeExceptionTest() throws IOException {
        // Rename a node
        renameNode();

        // Create another node
        NodeInfo folder6Info = storage.createNode(rootFolderInfo.getId(), "test6", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        String folder6NodeId = folder6Info.getId();
        storage.setConsistent(folder6NodeId);

        // Name null
        assertThrows(NullPointerException.class, () -> storage.renameNode(folder6NodeId, null));

        // Name empty
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> storage.renameNode(folder6NodeId, ""));
        assertTrue(IMPOSSIBLE_TO_RENAME_NODE_PATTERN.matcher(exception.getMessage()).matches());
    }

    @Test
    @Order(36)
    public void cascadingDeleteTest() throws IOException, InterruptedException {
        // Delete nodes in cascade
        prepareNodesForCascadeDeletion();
        clearEventStack();

        // All the deleted nodes are unavailable
        assertTrue(storage.getChildNode(rootFolderInfo.getId(), "test-delete").isPresent());
        String subFolderId = storage.getChildNode(rootFolderInfo.getId(), "test-delete").get().getId();
        assertTrue(storage.getChildNode(subFolderId, "sub-folder").isPresent());
        String subSubFolderId = storage.getChildNode(subFolderId, "sub-folder").get().getId();
        assertTrue(storage.getChildNode(subFolderId, "data1").isPresent());
        String data1Id = storage.getChildNode(subFolderId, "data1").get().getId();
        assertTrue(storage.getChildNode(subSubFolderId, "data1").isPresent());
        String data2Id = storage.getChildNode(subSubFolderId, "data1").get().getId();

        // Delete the nodes
        storage.deleteNode(subFolderId);
        storage.flush();

        // All the deleted nodes are unavailable
        AfsNodeNotFoundException exception;
        exception = assertThrows(AfsNodeNotFoundException.class, () -> storage.getNodeInfo(subFolderId));
        assertTrue(NODE_NOT_FOUND_PATTERN.matcher(exception.getMessage()).matches());
        exception = assertThrows(AfsNodeNotFoundException.class, () -> storage.getNodeInfo(subSubFolderId));
        assertTrue(NODE_NOT_FOUND_PATTERN.matcher(exception.getMessage()).matches());
        exception = assertThrows(AfsNodeNotFoundException.class, () -> storage.getNodeInfo(data1Id));
        assertTrue(NODE_NOT_FOUND_PATTERN.matcher(exception.getMessage()).matches());
        exception = assertThrows(AfsNodeNotFoundException.class, () -> storage.getNodeInfo(data2Id));
        assertTrue(NODE_NOT_FOUND_PATTERN.matcher(exception.getMessage()).matches());

        assertEventStackInAnyOrder(
            new NodeRemoved(subFolderId, rootFolderInfo.getId()),
            new NodeRemoved(subSubFolderId, subFolderId),
            new NodeRemoved(data1Id, subFolderId),
            new NodeRemoved(data2Id, subSubFolderId),
            new NodeDataRemoved(data1Id, "data1"),
            new NodeDataRemoved(data2Id, "data2")
        );
    }

    @Test
    @Order(37)
    public void otherTestsOnSameStructure() throws InterruptedException, IOException {
        // Initialize the nodes
        createAllNodesAsInTest();

        // Launch the remaining tests
        nextDependentTests();
    }

    @Test
    @Order(38)
    public void checkEventBusTest() throws InterruptedException {
        // Check that the event bus is defined
        assertNotNull(storage.getEventsBus());
        storage.flush();

        // Reset the bus
        clearEventStack();

        // Prepare to catch the event
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

        // Send the event
        storage.getEventsBus().pushEvent(eventNotToCatch, "other topic");
        storage.getEventsBus().pushEvent(eventToCatch, topic);
        storage.flush();

        // Check the events
        assertTrue(eventReceived.await(1000, TimeUnit.MILLISECONDS));
        assertThat(eventsCatched.get()).isNotNull();
        assertThat(eventsCatched.get().getTopic()).isEqualTo(topic);
        assertThat(eventsCatched.get().getEvents()).hasSize(1);
        assertThat(eventsCatched.get().getEvents().get(0)).isEqualTo(eventToCatch);
        assertEventStack(eventNotToCatch, eventToCatch);
    }

    @Test
    @Order(39)
    public void updateNodeMetadataEventTest() throws InterruptedException {
        // Create a metadata
        NodeGenericMetadata metadata = new NodeGenericMetadata();

        // Create a node using a clone of the metadata
        createRootFolder();
        NodeInfo node = storage.createNode(rootFolderInfo.getId(), "testNode", "unknownFile", "", 0, metadata);
        storage.setConsistent(node.getId());
        storage.flush();
        waitIfNeededForEventStack();
        clearEventStack();

        // Set a new metadata
        metadata.setString("test", "test");
        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();

        // Assert the event got catched
        assertEventStack(new NodeMetadataUpdated(node.getId(), metadata));
    }

    @Test
    @Order(40)
    public void updateNodeMetadataTest() {
        // Create a metadata
        NodeGenericMetadata metadata = new NodeGenericMetadata();

        // Create a node using a clone of the metadata
        createRootFolder();
        NodeInfo node = storage.createNode(rootFolderInfo.getId(), "testNode2", "unknownFile2", "", 0, cloneMetadata(metadata));
        storage.setConsistent(node.getId());
        storage.flush();
        waitIfNeededForEventStack();
        clearEventStack();

        // Assert the metadata is the same and is empty
        assertMetadataEquality(metadata, node.getGenericMetadata());
        assertThat(node.getGenericMetadata().getStrings().keySet()).isEmpty();

        // Change the metadata
        metadata.setString("test", "test");
        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();

        // Check the node
        node = storage.getNodeInfo(node.getId());
        assertMetadataEquality(metadata, node.getGenericMetadata());

        // Change the metadata again
        metadata.setBoolean("test1", true);
        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();

        // Check the node again
        node = storage.getNodeInfo(node.getId());
        assertMetadataEquality(metadata, node.getGenericMetadata());

        // Change the metadata again
        metadata.getStrings().remove("test");
        metadata.setDouble("test2", 0.1);
        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();

        // Check the node again
        node = storage.getNodeInfo(node.getId());
        assertMetadataEquality(metadata, node.getGenericMetadata());

        // Change the metadata again
        metadata.setInt("test3", 1);
        storage.setMetadata(node.getId(), cloneMetadata(metadata));
        storage.flush();

        // Check the node again
        node = storage.getNodeInfo(node.getId());
        assertMetadataEquality(metadata, node.getGenericMetadata());
    }

    private void clearEventStack() {
        eventStack.clear();
    }

    private void createRootFolder() {
        // Create the root folder
        rootFolderInfo = storage.createRootNodeIfNotExists(storage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        storage.flush();
    }

    private void createTestFolder() {
        // Previous things to do
        createRootFolder();
        waitIfNeededForEventStack();
        clearEventStack();

        // Create a test folder
        testFolderInfo = storage.createNode(rootFolderInfo.getId(), "test", FOLDER_PSEUDO_CLASS, "", 12,
            new NodeGenericMetadata().setString("k", "v"));
        storage.flush();
    }

    private void createConsistentTestFolder() {
        // Previous things to do
        createTestFolder();

        // Set the test folder consistent
        storage.setConsistent(testFolderInfo.getId());
        storage.flush();
    }

    private void setTestFolderDescription() {
        // Previous things to do
        createConsistentTestFolder();
        waitIfNeededForEventStack();
        clearEventStack();

        // Set the test folder description
        storage.setDescription(testFolderInfo.getId(), "hello");
        storage.flush();
    }

    private void updateModificationTimeOnTestFolder() {
        storage.updateModificationTime(testFolderInfo.getId());
        storage.flush();
    }

    private void createDataNodes() {
        // Previous things to do
        createConsistentTestFolder();
        waitIfNeededForEventStack();
        clearEventStack();

        // Set the metadata for the node 2
        NodeGenericMetadata metadata2 = new NodeGenericMetadata().setString("s1", "v1")
            .setDouble("d1", 1d)
            .setInt("i1", 2)
            .setBoolean("b1", false);

        // Create the 3 nodes
        NodeInfo testDataInfo = storage.createNode(testFolderInfo.getId(), "data", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo testData2Info = storage.createNode(testFolderInfo.getId(), "data2", DATA_FILE_CLASS, "", 0, metadata2);
        NodeInfo testData3Info = storage.createNode(testFolderInfo.getId(), "data3", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());

        // Set the 3 nodes consistent
        storage.setConsistent(testDataInfo.getId());
        storage.setConsistent(testData2Info.getId());
        storage.setConsistent(testData3Info.getId());
        storage.flush();

        dataNodeInfos = List.of(testDataInfo, testData2Info, testData3Info);
    }

    private void writeBinaryDataInNodes() {
        // Previous things to do
        createDataNodes();
        waitIfNeededForEventStack();
        clearEventStack();

        // Write the data
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(testFolderInfo.getId(), "testData1"), StandardCharsets.UTF_8)) {
            writer.write("Content for testData1");
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
    }

    private void createFirstDependency() {
        // Previous things to do
        createDataNodes();
        waitIfNeededForEventStack();
        clearEventStack();

        // Add a dependency
        storage.addDependency(dataNodeInfos.get(0).getId(), "mylink", dataNodeInfos.get(1).getId());
        storage.flush();
    }

    private String deleteNode() {
        // Previous things to do
        createFirstDependency();
        waitIfNeededForEventStack();
        clearEventStack();

        // Delete a node
        String deletedNodeId = storage.deleteNode(dataNodeInfos.get(0).getId());
        storage.flush();
        return deletedNodeId;
    }

    private void writeBlobInDataNode2() throws IOException {
        // Previous things to do
        deleteNode();
        waitIfNeededForEventStack();
        clearEventStack();

        // Write the first blob
        try (OutputStream os = storage.writeBinaryData(dataNodeInfos.get(1).getId(), "blob")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();
    }

    private void removeBlobInDataNode2() throws IOException {
        // Previous things to do
        writeBlobInDataNode2();
        waitIfNeededForEventStack();
        clearEventStack();

        // Remove blob
        storage.removeData(dataNodeInfos.get(1).getId(), "blob");
        storage.flush();
    }

    private TimeSeriesMetadata createTimeSeriesMetadata(String name, TimeSeriesDataType type, Map<String, String> tags) {
        // TimeSeries index
        TimeSeriesIndex index = RegularTimeSeriesIndex.create(Interval.parse("2015-01-01T00:00:00Z/2015-01-01T01:15:00Z"), Duration.ofMinutes(15));

        // Metadata
        return new TimeSeriesMetadata(name, type, tags, index);
    }

    private void createDoubleTimeSeries(TimeSeriesMetadata metadata) throws IOException {
        // Previous things to do
        removeBlobInDataNode2();
        waitIfNeededForEventStack();
        clearEventStack();

        // Create the timeseries
        storage.createTimeSeries(dataNodeInfos.get(1).getId(), metadata);
        storage.flush();
    }

    private void addDataToDoubleTimeSeries() throws IOException {
        // Previous things to do
        createDoubleTimeSeries(createTimeSeriesMetadata("ts1", TimeSeriesDataType.DOUBLE, Map.of("var1", "value1")));
        waitIfNeededForEventStack();
        clearEventStack();

        // Add data to the timeseries
        storage.addDoubleTimeSeriesData(dataNodeInfos.get(1).getId(), 0, "ts1",
            List.of(new CompressedDoubleDataChunk(2, 2, new double[] {1d, 2d}, new int[] {1, 1}),
                new UncompressedDoubleDataChunk(5, new double[] {3d})));
        storage.flush();
    }

    private void createStringTimeSeries(TimeSeriesMetadata metadata) throws IOException {
        // Previous things to do
        addDataToDoubleTimeSeries();
        waitIfNeededForEventStack();
        clearEventStack();

        // Create the timeseries
        storage.createTimeSeries(dataNodeInfos.get(1).getId(), metadata);
        storage.flush();
    }

    private void addDataToStringTimeSeries() throws IOException {
        // Previous things to do
        createStringTimeSeries(createTimeSeriesMetadata("ts2", TimeSeriesDataType.STRING, Map.of()));
        waitIfNeededForEventStack();
        clearEventStack();

        // Add data to the timeseries
        storage.addStringTimeSeriesData(dataNodeInfos.get(1).getId(), 0, "ts2",
            List.of(new CompressedStringDataChunk(2, 2, new String[] {"a", "b"}, new int[] {1, 1}),
                new UncompressedStringDataChunk(5, new String[] {"c"})));
        storage.flush();
    }

    private void clearTimeSeries() throws IOException {
        // Previous things to do
        addDataToStringTimeSeries();
        waitIfNeededForEventStack();
        clearEventStack();

        // Clear the timeseries
        storage.clearTimeSeries(dataNodeInfos.get(1).getId());
        storage.flush();
    }

    private void createFoldersPlusAFile() throws IOException {
        // Previous things to do
        clearTimeSeries();
        waitIfNeededForEventStack();
        clearEventStack();

        // Create two new folders
        NodeInfo folder1Info = storage.createNode(rootFolderInfo.getId(), "test1", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo folder2Info = storage.createNode(rootFolderInfo.getId(), "test2", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder1Info.getId());
        storage.setConsistent(folder2Info.getId());
        storage.flush();

        // create a file in folder 1
        NodeInfo fileInfo = storage.createNode(folder1Info.getId(), "file", "file-type", "", 0, new NodeGenericMetadata());
        storage.setConsistent(fileInfo.getId());
        storage.flush();
    }

    private void changeParent() {
        // Folders
        NodeInfo folder1Info = storage.getChildNode(rootFolderInfo.getId(), "test1").orElseThrow();
        NodeInfo folder2Info = storage.getChildNode(rootFolderInfo.getId(), "test2").orElseThrow();

        // File
        NodeInfo fileInfo = storage.getChildNode(folder1Info.getId(), "file").orElseThrow();

        // change parent to folder 2
        storage.setParentNode(fileInfo.getId(), folder2Info.getId());
        storage.flush();
    }

    private void createNodeForDeletion() throws IOException {
        // Previous things to do
        createFoldersPlusAFile();
        changeParent();
        waitIfNeededForEventStack();
        clearEventStack();

        // create 2 folders
        NodeInfo folder3Info = storage.createNode(rootFolderInfo.getId(), "test3", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo folder4Info = storage.createNode(rootFolderInfo.getId(), "test4", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder3Info.getId());
        storage.setConsistent(folder4Info.getId());
        storage.flush();

        // Add the dependency
        storage.addDependency(folder3Info.getId(), "dep", folder4Info.getId());
        storage.addDependency(folder3Info.getId(), "dep2", folder4Info.getId());
        storage.flush();
    }

    private void renameNode() throws IOException {
        // Previous things to do
        createNodeForDeletion();
        NodeInfo folder4Info = storage.getChildNode(rootFolderInfo.getId(), "test4").orElseThrow();
        storage.deleteNode(folder4Info.getId());
        waitIfNeededForEventStack();
        clearEventStack();

        // Metadata
        NodeGenericMetadata folder5Metadata = new NodeGenericMetadata().setString("k", "v");

        // Create a parent node
        NodeInfo folder5Info = storage.createNode(rootFolderInfo.getId(), "test5", FOLDER_PSEUDO_CLASS, "", 0, folder5Metadata);
        storage.setConsistent(folder5Info.getId());

        // Create 2 child nodes
        NodeInfo folder51Info = storage.createNode(folder5Info.getId(), "child_of_test5", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo folder52Info = storage.createNode(folder5Info.getId(), "another_child_of_test5", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder51Info.getId());
        storage.setConsistent(folder52Info.getId());
        storage.flush();

        // Rename the parent
        String newName = "newtest5";
        storage.renameNode(folder5Info.getId(), newName);
        storage.flush();
    }

    private void prepareNodesForCascadeDeletion() throws IOException {
        // Previous things to do
        renameNode();

        // Create some nodes
        NodeInfo subFolder = storage.createNode(rootFolderInfo.getId(), "test-delete", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(subFolder.getId());
        NodeInfo subSubFolder = storage.createNode(subFolder.getId(), "sub-folder", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(subSubFolder.getId());
        NodeInfo data1 = storage.createNode(subFolder.getId(), "data1", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(data1.getId());
        NodeInfo data2 = storage.createNode(subSubFolder.getId(), "data1", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(data2.getId());

        // Add some data
        try (OutputStream outputStream = storage.writeBinaryData(data1.getId(), "data1")) {
            outputStream.write("data1".getBytes(StandardCharsets.UTF_8));
        }
        try (OutputStream outputStream = storage.writeBinaryData(data2.getId(), "data2")) {
            outputStream.write("data2".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();
    }

    protected void createAllNodesAsInTest() throws IOException {
        prepareNodesForCascadeDeletion();
        waitIfNeededForEventStack();
        clearEventStack();
    }

    private void assertMetadataEquality(NodeGenericMetadata source, NodeGenericMetadata target) {
        assertThat(target).isNotNull();
        assertEquals(target.getBooleans().size(), source.getBooleans().size());
        source.getBooleans().forEach((key, val) -> assertThat(target.getBooleans()).contains(new HashMap.SimpleEntry<>(key, val)));
        assertEquals(target.getStrings().size(), source.getStrings().size());
        source.getStrings().forEach((key, val) -> assertThat(target.getStrings()).contains(new HashMap.SimpleEntry<>(key, val)));
        assertEquals(target.getInts().size(), source.getInts().size());
        source.getInts().forEach((key, val) -> assertThat(target.getInts()).contains(new HashMap.SimpleEntry<>(key, val)));
        assertEquals(target.getDoubles().size(), source.getDoubles().size());
        source.getDoubles().forEach((key, val) -> assertThat(target.getDoubles()).contains(new HashMap.SimpleEntry<>(key, val)));
    }

    private NodeGenericMetadata cloneMetadata(NodeGenericMetadata metadata) {
        NodeGenericMetadata clone = new NodeGenericMetadata();
        clone.getStrings().putAll(metadata.getStrings());
        clone.getBooleans().putAll(metadata.getBooleans());
        clone.getInts().putAll(metadata.getInts());
        clone.getDoubles().putAll(metadata.getDoubles());
        return clone;
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
}
