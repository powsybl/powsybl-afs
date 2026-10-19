/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.re2j.Pattern;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Abstract class used to test the multiple implementations of AppStorage
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
// Only one instance of the test class is created to avoid several initializations of the costly "storage" resource.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// Each tests are independent, but they follow a sequential logic.
// Having them ordered allow to easily identify the step to analyze when tests are failing (it's the 1st failing)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class AbstractAppStorageTest {

    protected static final String FOLDER_PSEUDO_CLASS = "folder";
    protected static final String DATA_FILE_CLASS = "data";

    private static final String NODE_NOT_FOUND_REGEX = "Node [0-9a-fA-F-]{36} not found";
    private static final Pattern NODE_NOT_FOUND_PATTERN = Pattern.compile(NODE_NOT_FOUND_REGEX);
    private static final String IMPOSSIBLE_TO_RENAME_NODE_REGEX = "Impossible to rename node '[0-9a-fA-F-]{36}' with an empty name";
    private static final Pattern IMPOSSIBLE_TO_RENAME_NODE_PATTERN = Pattern.compile(IMPOSSIBLE_TO_RENAME_NODE_REGEX);

    protected AppStorage storage;
    protected BlockingQueue<NodeEvent> eventStack;
    protected AppStorageListener l = eventList -> eventStack.addAll(eventList.getEvents());

    private NodeInfo rootFolderInfo;
    private NodeInfo test4FolderInfo;
    private NodeInfo test10FolderInfo;
    private NodeInfo test11FolderInfo;
    private NodeInfo test12FolderInfo;
    private NodeInfo test13FolderInfo;
    private NodeInfo test14FolderInfo;
    private NodeInfo test15FolderInfo;
    private NodeInfo test16FolderInfo;
    private NodeInfo test17FolderInfo;
    private NodeInfo test18FolderInfo;
    private NodeInfo test20FolderInfo;
    private NodeInfo test21FolderInfo;
    private NodeInfo test22FolderInfo;
    private NodeInfo test23FolderInfo;
    private NodeInfo test24FolderInfo;
    private NodeInfo test27FolderInfo;
    private NodeInfo test28FolderInfo;
    private NodeInfo test29FolderInfo;
    private NodeInfo test30FolderInfo;
    private NodeInfo test31FolderInfo;
    private NodeInfo test32FolderInfo;
    private NodeInfo test33FolderInfo;
    private NodeInfo test34FolderInfo;
    private NodeInfo test35FolderInfo;
    private NodeInfo test36FolderInfo;
    private NodeInfo test37FolderInfo;
    private NodeInfo test38FolderInfo;
    private NodeInfo test39FolderInfo;

    @BeforeEach
    public void setUp() throws IOException {
        if (this.storage == null) {
            this.storage = createStorage();
            createStorageStructure();
        }
        this.eventStack = new LinkedBlockingQueue<>();
        this.storage.getEventsBus().addListener(l);
    }

    @AfterEach
    public void tearDown() {
        storage.flush();
        clearEventStack();
    }

    @AfterAll
    public void closeStorage() {
        if (storage != null && !storage.isClosed()) {
            storage.close();
        }
    }

    @Test
    @Order(1)
    public void createRootFolderTest() throws InterruptedException {
        try (AppStorage localStorage = createStorage("rootTest")) {
            BlockingQueue<NodeEvent> localEventStack = new LinkedBlockingQueue<>();
            localStorage.getEventsBus().addListener(eventList -> localEventStack.addAll(eventList.getEvents()));

            // Create the root folder
            NodeInfo localRootFolderInfo = localStorage.createRootNodeIfNotExists(localStorage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
            localStorage.flush();

            // Assert root folder creation
            assertNotNull(localRootFolderInfo);

            // Assert node consistency
            assertTrue(localStorage.isConsistent(localRootFolderInfo.getId()));

            // check events
            assertEquals(new NodeCreated(localRootFolderInfo.getId(), null), localEventStack.take());
            assertEquals(new NodeConsistent(localRootFolderInfo.getId()), localEventStack.take());
            assertTrue(localEventStack.isEmpty());

            // assert root folder is writable
            assertTrue(localStorage.isWritable(localRootFolderInfo.getId()));

            // assert root folder parent is null
            assertFalse(localStorage.getParentNode(localRootFolderInfo.getId()).isPresent());

            // check root folder name and pseudo class is correct
            assertEquals(localStorage.getFileSystemName(), localStorage.getNodeInfo(localRootFolderInfo.getId()).getName());
            assertEquals(FOLDER_PSEUDO_CLASS, localStorage.getNodeInfo(localRootFolderInfo.getId()).getPseudoClass());

            // assert root folder is empty
            assertTrue(localStorage.getChildNodes(localRootFolderInfo.getId()).isEmpty());
        }
    }

    @Test
    @Order(2)
    public void createTestFolderTest() {
        // Create a test folder
        NodeInfo test2FolderInfo = storage.createNode(rootFolderInfo.getId(), "test2", FOLDER_PSEUDO_CLASS, "", 12,
            new NodeGenericMetadata().setString("k", "v"));
        storage.flush();

        // By default, the node should not be consistent
        assertFalse(storage.isConsistent(test2FolderInfo.getId()));
        assertEquals(1, storage.getInconsistentNodes().size());
        assertEquals(test2FolderInfo.getId(), storage.getInconsistentNodes().get(0).getId());

        // Delete the inconsistent node
        storage.deleteNode(test2FolderInfo.getId());
        storage.flush();
    }

    @Test
    @Order(3)
    public void createConsistentTestFolderTest() throws InterruptedException {
        // Create a test folder
        NodeInfo test3FolderInfo = storage.createNode(rootFolderInfo.getId(), "test3", FOLDER_PSEUDO_CLASS, "", 12,
            new NodeGenericMetadata().setString("k", "v"));
        storage.flush();

        // Set the test folder consistent
        storage.setConsistent(test3FolderInfo.getId());
        storage.flush();

        // The node should now be consistent
        assertTrue(storage.isConsistent(test3FolderInfo.getId()));
        assertTrue(storage.getInconsistentNodes().isEmpty());

        // Check events
        assertEventStack(new NodeCreated(test3FolderInfo.getId(), rootFolderInfo.getId()),
            new NodeConsistent(test3FolderInfo.getId()));
    }

    @Test
    @Order(4)
    public void getParentOnTestFolderTest() {
        // Assert parent of test folder is root folder
        assertEquals(rootFolderInfo, storage.getParentNode(test4FolderInfo.getId()).orElseThrow(AssertionError::new));
    }

    @Test
    @Order(5)
    public void getNodeInfoOnTestFolderTest() {
        // Retrieve the node info
        NodeInfo retrievedTestFolderInfo = storage.getNodeInfo(test4FolderInfo.getId());

        // Checks on the node info
        assertEquals(test4FolderInfo.getId(), retrievedTestFolderInfo.getId());
        assertEquals("test4", retrievedTestFolderInfo.getName());
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
        // Check test folder is empty
        assertTrue(storage.getChildNodes(test4FolderInfo.getId()).isEmpty());
    }

    @Test
    @Order(7)
    public void getChildNodesOnRootFolderAfterTestFolderCreationTest() {
        // This test suppose that only one test folder has been created so a new storage is used
        try (AppStorage localStorage = createStorage("rootTest")) {
            // Create the root folder
            NodeInfo localRootFolderInfo = localStorage.createRootNodeIfNotExists(localStorage.getFileSystemName(), FOLDER_PSEUDO_CLASS);

            // Create a test folder
            NodeInfo test7FolderInfo = localStorage.createNode(localRootFolderInfo.getId(), "test7", FOLDER_PSEUDO_CLASS, "", 12,
                new NodeGenericMetadata().setString("k", "v"));
            localStorage.flush();
            localStorage.setConsistent(test7FolderInfo.getId());
            localStorage.flush();

            // check root folder has one child (test folder)
            assertEquals(1, localStorage.getChildNodes(localRootFolderInfo.getId()).size());
            assertEquals(test7FolderInfo, localStorage.getChildNodes(localRootFolderInfo.getId()).get(0));
            assertTrue(localStorage.getChildNode(localRootFolderInfo.getId(), "test7").isPresent());
            assertEquals(test7FolderInfo, localStorage.getChildNode(localRootFolderInfo.getId(), "test7").orElseThrow(AssertionError::new));

            // check getChildNode return null if child does not exist
            assertFalse(localStorage.getChildNode(localRootFolderInfo.getId(), "???").isPresent());
        }
    }

    @Test
    @Order(8)
    public void setDescriptionOnTestFolderTest() throws InterruptedException {
        // Set the test folder description
        storage.setDescription(test4FolderInfo.getId(), "hello");
        storage.flush();

        // Check the description
        assertEquals("hello", storage.getNodeInfo(test4FolderInfo.getId()).getDescription());

        // check event
        assertEventStack(new NodeDescriptionUpdated(test4FolderInfo.getId(), "hello"));
    }

    @Test
    @Order(9)
    public void updateModificationTimeOnTestFolderTest() {
        // Get current modification time
        long oldModificationTime = test4FolderInfo.getModificationTime();

        // Update the date
        storage.updateModificationTime(test4FolderInfo.getId());
        storage.flush();

        // Compare the new date
        assertTrue(storage.getNodeInfo(test4FolderInfo.getId()).getModificationTime() >= oldModificationTime);
    }

    @Test
    @Order(10)
    public void createDataNodesTest() throws InterruptedException {
        // Set the metadata for the node 2
        NodeGenericMetadata metadata2 = new NodeGenericMetadata().setString("s1", "v1")
            .setDouble("d1", 1d)
            .setInt("i1", 2)
            .setBoolean("b1", false);

        // Create the 3 nodes
        NodeInfo testDataInfo = storage.createNode(test10FolderInfo.getId(), "data", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo testData2Info = storage.createNode(test10FolderInfo.getId(), "data2", DATA_FILE_CLASS, "", 0, metadata2);
        NodeInfo testData3Info = storage.createNode(test10FolderInfo.getId(), "data3", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());

        // Set the 3 nodes consistent
        storage.setConsistent(testDataInfo.getId());
        storage.setConsistent(testData2Info.getId());
        storage.setConsistent(testData3Info.getId());
        storage.flush();

        // Check events
        assertEventStack(new NodeCreated(testDataInfo.getId(), test10FolderInfo.getId()),
            new NodeCreated(testData2Info.getId(), test10FolderInfo.getId()),
            new NodeCreated(testData3Info.getId(), test10FolderInfo.getId()),
            new NodeConsistent(testDataInfo.getId()),
            new NodeConsistent(testData2Info.getId()),
            new NodeConsistent(testData3Info.getId()));

        // Check info are correctly stored even with metadata
        assertEquals(testData2Info, storage.getNodeInfo(testData2Info.getId()));

        // Check test folder has 3 children
        assertEquals(3, storage.getChildNodes(test10FolderInfo.getId()).size());

        // Check children names
        List<String> expectedChildrenNames = List.of("data", "data2", "data3");
        List<String> actualChildrenNames = storage.getChildNodes(test10FolderInfo.getId()).stream()
            .map(NodeInfo::getName).collect(Collectors.toList());
        assertEquals(expectedChildrenNames, actualChildrenNames);

        // Check data nodes initial dependency state
        assertTrue(storage.getDependencies(testDataInfo.getId()).isEmpty());
        assertTrue(storage.getDependencies(testData2Info.getId()).isEmpty());
        assertTrue(storage.getBackwardDependencies(testDataInfo.getId()).isEmpty());
        assertTrue(storage.getBackwardDependencies(testData2Info.getId()).isEmpty());
    }

    @Test
    @Order(11)
    public void noEventSentBeforeWriterStreamIsClosedTest() throws IOException, InterruptedException {
        // Check that no event is sent before the stream is closed
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(test11FolderInfo.getId(), "testData1"), StandardCharsets.UTF_8)) {
            writer.write("Content for testData1");
            storage.flush();

            //Event must not be sent before stream is closed: should still be empty for now
            assertEventStack();
        }
        storage.flush();
        assertEventStack(new NodeDataUpdated(test11FolderInfo.getId(), "testData1"));
    }

    @Test
    @Order(12)
    public void namedDataItemsCreationTest() throws InterruptedException {
        storage.flush();
        // Write the data
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(test12FolderInfo.getId(), "testData1"), StandardCharsets.UTF_8)) {
            writer.write("Content for testData1");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(test12FolderInfo.getId(), "testData2"), StandardCharsets.UTF_8)) {
            writer.write("Content for testData2");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(test12FolderInfo.getId(), "DATA_SOURCE_SUFFIX_EXT__Test3__ext"), StandardCharsets.UTF_8)) {
            writer.write("Content for DATA_SOURCE_SUFFIX_EXT__Test3__ext");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Writer writer = new OutputStreamWriter(storage.writeBinaryData(test12FolderInfo.getId(), "DATA_SOURCE_FILE_NAME__Test4"), StandardCharsets.UTF_8)) {
            writer.write("Content for DATA_SOURCE_FILE_NAME__Test4");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        storage.flush();

        // check events
        assertEventStack(new NodeDataUpdated(test12FolderInfo.getId(), "testData1"),
            new NodeDataUpdated(test12FolderInfo.getId(), "testData2"),
            new NodeDataUpdated(test12FolderInfo.getId(), "DATA_SOURCE_SUFFIX_EXT__Test3__ext"),
            new NodeDataUpdated(test12FolderInfo.getId(), "DATA_SOURCE_FILE_NAME__Test4"));

        // check data names
        assertEquals(Set.of("testData2", "testData1", "DATA_SOURCE_SUFFIX_EXT__Test3__ext", "DATA_SOURCE_FILE_NAME__Test4"),
            storage.getDataNames(test12FolderInfo.getId()));
    }

    @Test
    @Order(13)
    public void namedDataItemsCreationSeenFromDataSourceTest() throws IOException {
        // Create the datasource
        DataSource ds1 = new AppStorageDataSource(storage, test13FolderInfo.getId(), test13FolderInfo.getName());

        // check data names seen from data source
        assertEquals(Set.of("testData2", "testData1"), ds1.listNames("^testD.*"));
        AfsStorageException error = assertThrows(AfsStorageException.class, () -> ds1.listNames("^DATA_SOURCE_SUFFIX.*"));
        assertEquals("Don't know how to unmap suffix-and-extension to a data source name DATA_SOURCE_SUFFIX_EXT__Test3__ext",
            error.getMessage());
        assertEquals(Set.of("Test4"), ds1.listNames("^DATA_SOURCE_FILE.*"));
    }

    @Test
    @Order(14)
    public void firstDependencyCreationTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode1 = storage.getChildNode(test14FolderInfo.getId(), "data").orElseThrow();
        NodeInfo dataNode2 = storage.getChildNode(test14FolderInfo.getId(), "data2").orElseThrow();

        // Add a dependency
        storage.addDependency(dataNode1.getId(), "mylink", dataNode2.getId());
        storage.flush();

        // check event
        assertEventStack(new DependencyAdded(dataNode1.getId(), "mylink"),
            new BackwardDependencyAdded(dataNode2.getId(), "mylink"));

        // check dependency state
        assertEquals(Set.of(new NodeDependency("mylink", dataNode2)), storage.getDependencies(dataNode1.getId()));
        assertEquals(Set.of(dataNode1), storage.getBackwardDependencies(dataNode2.getId()));
        assertEquals(Set.of(dataNode2), storage.getDependencies(dataNode1.getId(), "mylink"));
        assertTrue(storage.getDependencies(dataNode1.getId(), "mylink2").isEmpty());
    }

    @Test
    @Order(15)
    public void secondDependencyCreationTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode1 = storage.getChildNode(test15FolderInfo.getId(), "data").orElseThrow();
        NodeInfo dataNode2 = storage.getChildNode(test15FolderInfo.getId(), "data2").orElseThrow();

        // Add the second dependency
        storage.addDependency(dataNode1.getId(), "mylink2", dataNode2.getId());
        storage.flush();

        // Check event
        assertEventStack(new DependencyAdded(dataNode1.getId(), "mylink2"),
            new BackwardDependencyAdded(dataNode2.getId(), "mylink2"));

        // Check the dependencies
        assertEquals(Set.of(new NodeDependency("mylink", dataNode2), new NodeDependency("mylink2", dataNode2)), storage.getDependencies(dataNode1.getId()));
        assertEquals(Set.of(dataNode1), storage.getBackwardDependencies(dataNode2.getId()));
        assertEquals(Set.of(dataNode2), storage.getDependencies(dataNode1.getId(), "mylink"));
    }

    @Test
    @Order(16)
    public void secondDependencyRemovalTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode1 = storage.getChildNode(test16FolderInfo.getId(), "data").orElseThrow();
        NodeInfo dataNode2 = storage.getChildNode(test16FolderInfo.getId(), "data2").orElseThrow();

        // Remove the second dependency
        storage.removeDependency(dataNode1.getId(), "mylink2", dataNode2.getId());
        storage.flush();

        // check event
        assertEventStack(new DependencyRemoved(dataNode1.getId(), "mylink2"),
            new BackwardDependencyRemoved(dataNode2.getId(), "mylink2"));

        assertEquals(Set.of(new NodeDependency("mylink", dataNode2)), storage.getDependencies(dataNode1.getId()));
        assertEquals(Set.of(dataNode1), storage.getBackwardDependencies(dataNode2.getId()));
        assertEquals(Set.of(dataNode2), storage.getDependencies(dataNode1.getId(), "mylink"));
    }

    @Test
    @Order(17)
    public void dataNodeDeletionTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode1 = storage.getChildNode(test17FolderInfo.getId(), "data").orElseThrow();
        NodeInfo dataNode2 = storage.getChildNode(test17FolderInfo.getId(), "data2").orElseThrow();

        // Delete a node
        String deletedNodeId = storage.deleteNode(dataNode1.getId());
        storage.flush();

        // Check the ID (deleteNode returns the id of the deleted node parent)
        assertEquals(test17FolderInfo.getId(), deletedNodeId);

        // check event
        assertEventStack(
            new DependencyRemoved(dataNode1.getId(), "mylink"),
            new BackwardDependencyRemoved(dataNode2.getId(), "mylink"),
            new NodeRemoved(dataNode1.getId(), test17FolderInfo.getId()));

        // check test folder children have been correctly updated
        assertEquals(2, storage.getChildNodes(test17FolderInfo.getId()).size());

        // check data node 2 backward dependency has been correctly updated
        assertTrue(storage.getBackwardDependencies(dataNode2.getId()).isEmpty());
    }

    @Test
    @Order(18)
    public void dataNodeMetadataValuesTest() {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test18FolderInfo.getId(), "data2").orElseThrow();

        // Check the metadata values
        assertEquals(Map.of("s1", "v1"), dataNode.getGenericMetadata().getStrings());
        assertEquals(Map.of("d1", 1d), dataNode.getGenericMetadata().getDoubles());
        assertEquals(Map.of("i1", 2), dataNode.getGenericMetadata().getInts());
        assertEquals(Map.of("b1", false), dataNode.getGenericMetadata().getBooleans());
    }

    @Test
    @Order(19)
    public void compareNodeToItselfFromParentTest() {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test18FolderInfo.getId(), "data2").orElseThrow();

        // Check that the child from the parent of a node is the same node
        assertEqualsFromParent(dataNode);
    }

    @Test
    @Order(20)
    public void writeBlobInDataNodeTest() throws IOException, InterruptedException {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test20FolderInfo.getId(), "data2").orElseThrow();

        // Write the first blob
        try (OutputStream os = storage.writeBinaryData(dataNode.getId(), "blob")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        // check event
        assertEventStack(new NodeDataUpdated(dataNode.getId(), "blob"));

        // Check blob
        try (InputStream is = storage.readBinaryData(dataNode.getId(), "blob").orElseThrow(AssertionError::new)) {
            assertEquals("word2", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));

        }
        assertTrue(storage.dataExists(dataNode.getId(), "blob"));
        assertFalse(storage.dataExists(dataNode.getId(), "blob2"));
        assertEquals(Set.of("blob"), storage.getDataNames(dataNode.getId()));
    }

    @Test
    @Order(21)
    public void removeInexistentBlobTest() {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test21FolderInfo.getId(), "data2").orElseThrow();

        // Try to remove a non-existing blob
        assertFalse(storage.removeData(dataNode.getId(), "blob2"));
    }

    @Test
    @Order(22)
    public void removeExistingBlobTest() {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test22FolderInfo.getId(), "data2").orElseThrow();

        // Try to remove an existing blob
        assertTrue(storage.removeData(dataNode.getId(), "blob"));
        storage.flush();
    }

    @Test
    @Order(23)
    public void removeBlobInDataNodeTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test23FolderInfo.getId(), "data2").orElseThrow();

        // Remove an existing blob
        storage.removeData(dataNode.getId(), "blob");
        storage.flush();

        // check event
        assertEventStack(new NodeDataRemoved(dataNode.getId(), "blob"));

        assertTrue(storage.getDataNames(dataNode.getId()).isEmpty());
        assertFalse(storage.readBinaryData(dataNode.getId(), "blob").isPresent());
    }

    @Test
    @Order(24)
    public void datasourceOnDataNodePatternTest() throws IOException, InterruptedException {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test24FolderInfo.getId(), "data2").orElseThrow();

        // Create the datasource
        DataSource ds = new AppStorageDataSource(storage, dataNode.getId(), dataNode.getName());

        // Base name
        assertEquals(dataNode.getName(), ds.getBaseName());

        // The file does not yet exist
        assertFalse(ds.exists(null, "ext"));

        // Write a new file
        try (OutputStream os = ds.newOutputStream(null, "ext", false)) {
            os.write("word1".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        // check event
        assertEventStack(new NodeDataUpdated(dataNode.getId(), "DATA_SOURCE_SUFFIX_EXT____ext"));

        // The file now exists
        assertTrue(ds.exists(null, "ext"));

        // Read the file
        try (InputStream is = ds.newInputStream(null, "ext")) {
            assertEquals("word1", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
        }
    }

    @Test
    @Order(25)
    public void datasourceByPatternInputStreamOnNonExistingFileExceptionTest() {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test24FolderInfo.getId(), "data2").orElseThrow();

        // Create the datasource
        DataSource ds = new AppStorageDataSource(storage, dataNode.getId(), dataNode.getName());

        // The datasource cannot read a non-existing file
        try (InputStream ignored = ds.newInputStream(null, "ext2")) {
            fail();
        } catch (IOException exception) {
            assertInstanceOf(IOException.class, exception);
            assertEquals("*.ext2 does not exist", exception.getMessage());
        }
    }

    @Test
    @Order(26)
    public void datasourceOnDataNodeFileNameTest() throws IOException, InterruptedException {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test24FolderInfo.getId(), "data2").orElseThrow();

        // Create the datasource
        DataSource ds = new AppStorageDataSource(storage, dataNode.getId(), dataNode.getName());

        // The file does not yet exist
        assertFalse(ds.exists("file1"));

        // Write a new file
        try (OutputStream os = ds.newOutputStream("file1", false)) {
            os.write("word1".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        // check event
        assertEventStack(new NodeDataUpdated(dataNode.getId(), "DATA_SOURCE_FILE_NAME__file1"));

        // The file now exists
        assertTrue(ds.exists("file1"));

        // Read the file
        try (InputStream is = ds.newInputStream("file1")) {
            assertEquals("word1", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
        }
    }

    @Test
    @Order(27)
    public void datasourceByFilenameInputStreamOnNonExistingFileExceptionTest() {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test27FolderInfo.getId(), "data2").orElseThrow();

        // Create the datasource
        DataSource ds = new AppStorageDataSource(storage, dataNode.getId(), dataNode.getName());

        // The datasource cannot read a non-existing file
        try (InputStream ignored = ds.newInputStream("file1")) {
            fail();
        } catch (IOException exception) {
            assertInstanceOf(IOException.class, exception);
            assertEquals("file1 does not exist", exception.getMessage());
        }
    }

    @Test
    @Order(28)
    public void createDoubleTimeSeriesTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode2 = storage.getChildNode(test28FolderInfo.getId(), "data2").orElseThrow();
        NodeInfo dataNode3 = storage.getChildNode(test28FolderInfo.getId(), "data3").orElseThrow();

        // Create the metadata
        TimeSeriesMetadata metadata = createTimeSeriesMetadata("ts1", TimeSeriesDataType.DOUBLE, Map.of("var1", "value1"));

        // Create the timeseries
        storage.createTimeSeries(dataNode2.getId(), metadata);
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesCreated(dataNode2.getId(), "ts1"));

        // check double time series query
        assertEquals(Sets.newHashSet("ts1"), storage.getTimeSeriesNames(dataNode2.getId()));
        assertTrue(storage.timeSeriesExists(dataNode2.getId(), "ts1"));
        assertFalse(storage.timeSeriesExists(dataNode2.getId(), "ts9"));
        assertFalse(storage.timeSeriesExists(dataNode3.getId(), "ts1"));

        // Check the metadata
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(dataNode2.getId(), Sets.newHashSet("ts1"));
        assertEquals(1, metadataList.size());
        assertEquals(metadata, metadataList.get(0));
        assertTrue(storage.getTimeSeriesMetadata(dataNode3.getId(), Sets.newHashSet("ts1")).isEmpty());
    }

    @Test
    @Order(29)
    public void addDataToDoubleTimeSeriesTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode2 = storage.getChildNode(test29FolderInfo.getId(), "data2").orElseThrow();
        NodeInfo dataNode3 = storage.getChildNode(test29FolderInfo.getId(), "data3").orElseThrow();

        // Add data to the timeseries
        storage.addDoubleTimeSeriesData(dataNode2.getId(), 0, "ts1",
            List.of(new CompressedDoubleDataChunk(2, 2, new double[] {1d, 2d}, new int[] {1, 1}),
                new UncompressedDoubleDataChunk(5, new double[] {3d})));
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesDataUpdated(dataNode2.getId(), "ts1"));

        // check versions
        assertEquals(Set.of(0), storage.getTimeSeriesDataVersions(dataNode2.getId()));
        assertEquals(Set.of(0), storage.getTimeSeriesDataVersions(dataNode2.getId(), "ts1"));

        // check double time series data query
        Map<String, List<DoubleDataChunk>> doubleTimeSeriesData = storage.getDoubleTimeSeriesData(dataNode2.getId(), Sets.newHashSet("ts1"), 0);
        assertEquals(1, doubleTimeSeriesData.size());
        List<DoubleDataChunk> doubleDataChunks = List.of(new CompressedDoubleDataChunk(2, 2, new double[] {1d, 2d}, new int[] {1, 1}),
            new UncompressedDoubleDataChunk(5, new double[] {3d}));
        assertEquals(doubleDataChunks, doubleTimeSeriesData.get("ts1"));

        // The result is empty if the timeseries does not exist
        assertTrue(storage.getDoubleTimeSeriesData(dataNode3.getId(), Sets.newHashSet("ts1"), 0).isEmpty());
    }

    @Test
    @Order(30)
    public void createStringTimeSeriesTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode2 = storage.getChildNode(test30FolderInfo.getId(), "data2").orElseThrow();

        // Create the metadata
        TimeSeriesMetadata metadata = createTimeSeriesMetadata("ts2", TimeSeriesDataType.STRING, Map.of("var1", "value1"));

        // Create the timeseries
        storage.createTimeSeries(dataNode2.getId(), metadata);
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesCreated(dataNode2.getId(), "ts2"));

        // check string time series query
        assertEquals(Sets.newHashSet("ts1", "ts2"), storage.getTimeSeriesNames(dataNode2.getId()));
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(dataNode2.getId(), Sets.newHashSet("ts1"));
        assertEquals(1, metadataList.size());
    }

    @Test
    @Order(31)
    public void addDataToStringTimeSeriesTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test31FolderInfo.getId(), "data2").orElseThrow();

        // Add data to the timeseries
        storage.addStringTimeSeriesData(dataNode.getId(), 0, "ts2",
            List.of(new CompressedStringDataChunk(2, 2, new String[] {"a", "b"}, new int[] {1, 1}),
                new UncompressedStringDataChunk(5, new String[] {"c"})));
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesDataUpdated(dataNode.getId(), "ts2"));

        // check string time series data query
        Map<String, List<StringDataChunk>> stringTimeSeriesData = storage.getStringTimeSeriesData(dataNode.getId(), Sets.newHashSet("ts2"), 0);
        assertEquals(1, stringTimeSeriesData.size());
        List<StringDataChunk> stringDataChunks = List.of(new CompressedStringDataChunk(2, 2, new String[] {"a", "b"}, new int[] {1, 1}),
            new UncompressedStringDataChunk(5, new String[] {"c"}));
        assertEquals(stringDataChunks, stringTimeSeriesData.get("ts2"));
    }

    @Test
    @Order(32)
    public void clearTimeSeriesTest() throws InterruptedException {
        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(test32FolderInfo.getId(), "data2").orElseThrow();

        // Clear the timeseries
        storage.clearTimeSeries(dataNode.getId());
        storage.flush();

        // check event
        assertEventStack(new TimeSeriesCleared(dataNode.getId()));

        // check there is no more time series
        assertTrue(storage.getTimeSeriesNames(dataNode.getId()).isEmpty());
    }

    @Test
    @Order(33)
    public void changeParentTest() throws InterruptedException {
        // Folders
        NodeInfo folder1Info = storage.getChildNode(test33FolderInfo.getId(), "test1").orElseThrow();
        NodeInfo folder2Info = storage.getChildNode(test33FolderInfo.getId(), "test2").orElseThrow();

        // File
        NodeInfo fileInfo = storage.getChildNode(folder1Info.getId(), "file").orElseThrow();

        // check parent folder
        assertEquals(folder1Info, storage.getParentNode(fileInfo.getId()).orElseThrow());

        // Change parent to folder 2
        storage.setParentNode(fileInfo.getId(), folder2Info.getId());
        storage.flush();

        // check event
        assertEventStack(new ParentChanged(fileInfo.getId(), folder1Info.getId(), folder2Info.getId()));

        // check parent folder change
        assertEquals(folder2Info, storage.getParentNode(fileInfo.getId()).orElseThrow());
    }

    @Test
    @Order(34)
    public void deleteNodeTest() {
        // create 2 folders
        NodeInfo folder1Info = storage.createNode(test34FolderInfo.getId(), "test3", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo folder2Info = storage.createNode(test34FolderInfo.getId(), "test4", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder1Info.getId());
        storage.setConsistent(folder2Info.getId());
        storage.flush();

        // Add the dependency
        storage.addDependency(folder1Info.getId(), "dep", folder2Info.getId());
        storage.addDependency(folder1Info.getId(), "dep2", folder2Info.getId());
        storage.flush();

        // Check that the dependencies exist
        assertEquals(Collections.singleton(folder2Info), storage.getDependencies(folder1Info.getId(), "dep"));
        assertEquals(Collections.singleton(folder2Info), storage.getDependencies(folder1Info.getId(), "dep2"));

        // Delete the node 4
        storage.deleteNode(folder2Info.getId());

        // Check that the dependencies do not exist anymore
        assertTrue(storage.getDependencies(folder1Info.getId(), "dep").isEmpty());
        assertTrue(storage.getDependencies(folder1Info.getId(), "dep2").isEmpty());
    }

    @Test
    @Order(35)
    public void renameNodeTest() {
        // Get the node
        NodeInfo folder5Info = storage.getChildNode(test35FolderInfo.getId(), "test5").orElseThrow(AssertionError::new);

        // Rename the parent
        storage.renameNode(folder5Info.getId(), "newtest5");
        storage.flush();

        // Get the node again
        folder5Info = storage.getChildNode(test35FolderInfo.getId(), "newtest5").orElseThrow(AssertionError::new);

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
    @Order(36)
    public void renameNodeExceptionTest() {
        // Get the node
        NodeInfo folderInfo = storage.getChildNode(test36FolderInfo.getId(), "test5").orElseThrow(AssertionError::new);
        String folderInfoId = folderInfo.getId();

        // Name null
        assertThrows(NullPointerException.class, () -> storage.renameNode(folderInfoId, null));

        // Name empty
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> storage.renameNode(folderInfoId, ""));
        assertTrue(IMPOSSIBLE_TO_RENAME_NODE_PATTERN.matcher(exception.getMessage()).matches());
    }

    @Test
    @Order(37)
    public void cascadingDeleteTest() throws InterruptedException {
        // All the nodes are available
        assertTrue(storage.getChildNode(test37FolderInfo.getId(), "test-delete").isPresent());
        String subFolderId = storage.getChildNode(test37FolderInfo.getId(), "test-delete").get().getId();
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
            new NodeRemoved(subFolderId, test37FolderInfo.getId()),
            new NodeRemoved(subSubFolderId, subFolderId),
            new NodeRemoved(data1Id, subFolderId),
            new NodeRemoved(data2Id, subSubFolderId),
            new NodeDataRemoved(data1Id, "data1"),
            new NodeDataRemoved(data2Id, "data2")
        );
    }

    @Test
    @Order(38)
    public void updateNodeMetadataEventTest() throws InterruptedException {
        // Set a new metadata
        NodeGenericMetadata metadata = new NodeGenericMetadata();
        metadata.setString("test", "test");
        storage.setMetadata(test38FolderInfo.getId(), cloneMetadata(metadata));
        storage.flush();

        // Assert the event got catched
        assertEventStack(new NodeMetadataUpdated(test38FolderInfo.getId(), metadata));
    }

    @Test
    @Order(39)
    public void updateNodeMetadataTest() {
        // Create a metadata
        NodeGenericMetadata metadata = new NodeGenericMetadata();

        // Assert the metadata is the same and is empty
        assertMetadataEquality(metadata, test39FolderInfo.getGenericMetadata());
        assertThat(test39FolderInfo.getGenericMetadata().getStrings().keySet()).isEmpty();

        // Change the metadata
        metadata.setString("test", "test");
        storage.setMetadata(test39FolderInfo.getId(), cloneMetadata(metadata));
        storage.flush();

        // Check the node
        test39FolderInfo = storage.getNodeInfo(test39FolderInfo.getId());
        assertMetadataEquality(metadata, test39FolderInfo.getGenericMetadata());

        // Change the metadata again, to test another value type
        metadata.setBoolean("test1", true);
        storage.setMetadata(test39FolderInfo.getId(), cloneMetadata(metadata));
        storage.flush();

        // Check the node again
        test39FolderInfo = storage.getNodeInfo(test39FolderInfo.getId());
        assertMetadataEquality(metadata, test39FolderInfo.getGenericMetadata());

        // Change the metadata again, to test other value modifications
        metadata.getStrings().remove("test");
        metadata.setDouble("test2", 0.1);
        storage.setMetadata(test39FolderInfo.getId(), cloneMetadata(metadata));
        storage.flush();

        // Check the node again
        test39FolderInfo = storage.getNodeInfo(test39FolderInfo.getId());
        assertMetadataEquality(metadata, test39FolderInfo.getGenericMetadata());

        // Change the metadata again, to test int type
        metadata.setInt("test3", 1);
        storage.setMetadata(test39FolderInfo.getId(), cloneMetadata(metadata));
        storage.flush();

        // Check the node again
        test39FolderInfo = storage.getNodeInfo(test39FolderInfo.getId());
        assertMetadataEquality(metadata, test39FolderInfo.getGenericMetadata());
    }

    @Test
    @Order(40)
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
    @Order(41)
    public void otherTestsOnSameStructure() throws InterruptedException {
        // Check the test folder
        Optional<NodeInfo> testFolderId = storage.getChildNode(rootFolderInfo.getId(), "other-tests");
        assertTrue(testFolderId.isPresent());

        // Launch the remaining tests
        nextDependentTests();
    }

    protected abstract AppStorage createStorage();

    protected abstract AppStorage createStorage(String fileSystemName);

    protected void nextDependentTests() throws InterruptedException {
        // Noop
        // allow sub classes to continue tests using created node root
    }

    private void clearEventStack() {
        eventStack.clear();
    }

    private void createStorageStructure() throws IOException {
        // Test 2, 3, 41
        rootFolderInfo = storage.createRootNodeIfNotExists(storage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        storage.flush();

        // Tests 4, 5, 6, 8, 9
        test4FolderInfo = createTestFolder("test4");

        // Test 10
        test10FolderInfo = createTestFolder("test10");

        // Tests 11
        test11FolderInfo = createDataNodes("test11");

        // Tests 12
        test12FolderInfo = createDataNodes("test12");

        // Test 13
        test13FolderInfo = writeBinaryDataInNodes();

        // Test 14
        test14FolderInfo = createDataNodes("test14");

        // Test 15
        test15FolderInfo = createFirstDependency("test15");

        // Test 16
        test16FolderInfo = createSecondDependency();

        // Test 17
        test17FolderInfo = createFirstDependency("test17");

        // Test 18, 19
        test18FolderInfo = deleteNode("test18");

        // Test 20
        test20FolderInfo = deleteNode("test20");

        // Test 21
        test21FolderInfo = writeBlobInDataNode2("test21");

        // Test 22
        test22FolderInfo = writeBlobInDataNode2("test22");

        // Test 23
        test23FolderInfo = writeBlobInDataNode2("test23");

        // Tests 24, 25, 26
        test24FolderInfo = removeBlobInDataNode2("test24");

        // Tests 27
        test27FolderInfo = removeBlobInDataNode2("test27");

        // Test 28
        test28FolderInfo = removeBlobInDataNode2("test28");

        // Test 29
        test29FolderInfo = createTimeSeries("test29");

        // Test 30
        test30FolderInfo = addDataToDoubleTimeSeries("test30");

        // Test 31
        test31FolderInfo = createStringTimeSeries();

        // Test 32
        test32FolderInfo = addDataToStringTimeSeries("test32");

        // Test 33
        test33FolderInfo = createFoldersPlusAFile();

        // Test 34
        test34FolderInfo = createTestFolder("test34");

        // Test 35
        test35FolderInfo = createNodeToRename("test35");

        // Test 36
        test36FolderInfo = createNodeToRename("test36");

        // Test 37
        test37FolderInfo = prepareNodesForCascadeDeletion();

        // Test 38
        test38FolderInfo = createTestFolder("test38");

        // Test 39
        test39FolderInfo = createTestFolder("test39", new NodeGenericMetadata());

        // Other tests
        addDataToStringTimeSeries("other-tests");
    }

    private NodeInfo createTestFolder(String folderName) {
        return createTestFolder(folderName, new NodeGenericMetadata().setString("k", "v"));
    }

    private NodeInfo createTestFolder(String folderName, NodeGenericMetadata metadata) {
        NodeInfo folderInfo = storage.createNode(rootFolderInfo.getId(), folderName, FOLDER_PSEUDO_CLASS, "", 12,
            metadata);
        storage.flush();
        storage.setConsistent(folderInfo.getId());
        storage.flush();

        return folderInfo;
    }

    private NodeInfo createDataNodes(String folderName) {
        NodeInfo testFolderInfo = createTestFolder(folderName);

        // Set the metadata for the node 2
        NodeGenericMetadata metadata = new NodeGenericMetadata().setString("s1", "v1")
            .setDouble("d1", 1d)
            .setInt("i1", 2)
            .setBoolean("b1", false);

        // Create the 3 nodes
        NodeInfo testDataInfo = storage.createNode(testFolderInfo.getId(), "data", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo testData2Info = storage.createNode(testFolderInfo.getId(), "data2", DATA_FILE_CLASS, "", 0, metadata);
        NodeInfo testData3Info = storage.createNode(testFolderInfo.getId(), "data3", DATA_FILE_CLASS, "", 0, new NodeGenericMetadata());

        // Set the 3 nodes consistent
        storage.setConsistent(testDataInfo.getId());
        storage.setConsistent(testData2Info.getId());
        storage.setConsistent(testData3Info.getId());
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo writeBinaryDataInNodes() {
        NodeInfo testFolderInfo = createDataNodes("test13");

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
        return testFolderInfo;
    }

    private NodeInfo createFirstDependency(String folderName) {
        NodeInfo testFolderInfo = createDataNodes(folderName);

        // Get the data nodes
        NodeInfo dataNode1 = storage.getChildNode(testFolderInfo.getId(), "data").orElseThrow();
        NodeInfo dataNode2 = storage.getChildNode(testFolderInfo.getId(), "data2").orElseThrow();

        // Add a dependency
        storage.addDependency(dataNode1.getId(), "mylink", dataNode2.getId());
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo createSecondDependency() {
        NodeInfo testFolderInfo = createFirstDependency("test16");

        // Get the data nodes
        NodeInfo dataNode1 = storage.getChildNode(testFolderInfo.getId(), "data").orElseThrow();
        NodeInfo dataNode2 = storage.getChildNode(testFolderInfo.getId(), "data2").orElseThrow();

        // Add a dependency
        storage.addDependency(dataNode1.getId(), "mylink2", dataNode2.getId());
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo deleteNode(String folderName) {
        NodeInfo testFolderInfo = createFirstDependency(folderName);

        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(testFolderInfo.getId(), "data").orElseThrow();

        // Delete a node
        storage.deleteNode(dataNode.getId());
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo writeBlobInDataNode2(String folderName) throws IOException {
        NodeInfo testFolderInfo = deleteNode(folderName);

        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(testFolderInfo.getId(), "data2").orElseThrow();

        // Write the first blob
        try (OutputStream os = storage.writeBinaryData(dataNode.getId(), "blob")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo removeBlobInDataNode2(String folderName) throws IOException {
        NodeInfo testFolderInfo = writeBlobInDataNode2(folderName);

        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(testFolderInfo.getId(), "data2").orElseThrow();

        // Remove blob
        storage.removeData(dataNode.getId(), "blob");
        storage.flush();

        return testFolderInfo;
    }

    private TimeSeriesMetadata createTimeSeriesMetadata(String name, TimeSeriesDataType type, Map<String, String> tags) {
        // TimeSeries index
        TimeSeriesIndex index = RegularTimeSeriesIndex.create(Interval.parse("2015-01-01T00:00:00Z/2015-01-01T01:15:00Z"), Duration.ofMinutes(15));

        // Metadata
        return new TimeSeriesMetadata(name, type, tags, index);
    }

    private NodeInfo createTimeSeries(String folderName) throws IOException {
        NodeInfo testFolderInfo = removeBlobInDataNode2(folderName);

        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(testFolderInfo.getId(), "data2").orElseThrow();

        // Create the metadata
        TimeSeriesMetadata metadata = createTimeSeriesMetadata("ts1", TimeSeriesDataType.DOUBLE, Map.of("var1", "value1"));

        // Create the timeseries
        storage.createTimeSeries(dataNode.getId(), metadata);
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo addDataToDoubleTimeSeries(String folderName) throws IOException {
        NodeInfo testFolderInfo = createTimeSeries(folderName);

        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(testFolderInfo.getId(), "data2").orElseThrow();

        // Add data to the timeseries
        storage.addDoubleTimeSeriesData(dataNode.getId(), 0, "ts1",
            List.of(new CompressedDoubleDataChunk(2, 2, new double[] {1d, 2d}, new int[] {1, 1}),
                new UncompressedDoubleDataChunk(5, new double[] {3d})));
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo createStringTimeSeries() throws IOException {
        NodeInfo testFolderInfo = addDataToDoubleTimeSeries("test31");

        // Get the data nodes
        NodeInfo dataNode2 = storage.getChildNode(testFolderInfo.getId(), "data2").orElseThrow();

        // Create the metadata
        TimeSeriesMetadata metadata = createTimeSeriesMetadata("ts2", TimeSeriesDataType.STRING, Map.of("var1", "value1"));

        // Create the timeseries
        storage.createTimeSeries(dataNode2.getId(), metadata);
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo addDataToStringTimeSeries(String folderName) throws IOException {
        NodeInfo testFolderInfo = addDataToDoubleTimeSeries(folderName);

        // Get the data nodes
        NodeInfo dataNode = storage.getChildNode(testFolderInfo.getId(), "data2").orElseThrow();

        // Add data to the timeseries
        storage.addStringTimeSeriesData(dataNode.getId(), 0, "ts2",
            List.of(new CompressedStringDataChunk(2, 2, new String[] {"a", "b"}, new int[] {1, 1}),
                new UncompressedStringDataChunk(5, new String[] {"c"})));
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo createFoldersPlusAFile() {
        NodeInfo testFolderInfo = createTestFolder("test33");

        // Create two new folders
        NodeInfo folder1Info = storage.createNode(testFolderInfo.getId(), "test1", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo folder2Info = storage.createNode(testFolderInfo.getId(), "test2", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(folder1Info.getId());
        storage.setConsistent(folder2Info.getId());
        storage.flush();

        // create a file in folder 1
        NodeInfo fileInfo = storage.createNode(folder1Info.getId(), "file", "file-type", "", 0, new NodeGenericMetadata());
        storage.setConsistent(fileInfo.getId());
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo createNodeToRename(String folderName) {
        NodeInfo testFolderInfo = createTestFolder(folderName);

        // Metadata
        NodeGenericMetadata folder5Metadata = new NodeGenericMetadata().setString("k", "v");

        // Create a parent node
        NodeInfo parentFolderInfo = storage.createNode(testFolderInfo.getId(), "test5", FOLDER_PSEUDO_CLASS, "", 0, folder5Metadata);
        storage.setConsistent(parentFolderInfo.getId());
        storage.flush();

        // Create 2 child nodes
        NodeInfo child1FolderInfo = storage.createNode(parentFolderInfo.getId(), "child_of_test5", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        NodeInfo child2FolderInfo = storage.createNode(parentFolderInfo.getId(), "another_child_of_test5", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(child1FolderInfo.getId());
        storage.setConsistent(child2FolderInfo.getId());
        storage.flush();

        return testFolderInfo;
    }

    private NodeInfo prepareNodesForCascadeDeletion() throws IOException {
        NodeInfo testFolderInfo = createTestFolder("test37");

        // Create some nodes
        NodeInfo subFolder = storage.createNode(testFolderInfo.getId(), "test-delete", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
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

        return testFolderInfo;
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
