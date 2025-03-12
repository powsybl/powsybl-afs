/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.local.storage;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.Folder;
import com.powsybl.afs.ext.base.Case;
import com.powsybl.afs.ext.base.TestImporter;
import com.powsybl.afs.storage.AppStorageDataSource;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.commons.datasource.DataSource;
import com.powsybl.computation.ComputationManager;
import com.powsybl.iidm.network.ImportConfig;
import com.powsybl.iidm.network.ImportersLoaderList;
import com.powsybl.iidm.network.Network;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.InfiniteTimeSeriesIndex;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesDataType;
import com.powsybl.timeseries.TimeSeriesMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.powsybl.afs.local.storage.LocalAppStorage.METHOD_NOT_IMPLEMENTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class LocalAppStorageTest {

    private FileSystem fileSystem;

    private LocalAppStorage storage;

    @BeforeEach
    void setUp() throws Exception {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());

        // Create the root directory
        Path rootDir = fileSystem.getPath("/cases");
        Files.createDirectories(rootDir);

        // Create two files in the root directory
        Path path1 = rootDir.resolve("n.tst");
        Path path2 = rootDir.resolve("n2.tst");
        Files.createFile(path1);
        Files.createFile(path2);

        // Create the LocalAppStorage
        ComputationManager computationManager = mock(ComputationManager.class);
        Network network = mock(Network.class);
        List<LocalFileScanner> fileExtensions
            = Collections.singletonList(new LocalCaseScanner(new ImportConfig(), new ImportersLoaderList(new TestImporter(network))));
        storage = new LocalAppStorage(rootDir, "mem", fileExtensions, Collections.emptyList(), computationManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
        fileSystem.close();
    }

    @Test
    void testConsistent() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);
        String id = rootNodeInfo.getId();
        assertThrows(AfsException.class, () -> storage.setConsistent(id));
    }

    @Test
    void rootNodeInfoTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);
        assertEquals("mem", rootNodeInfo.getName());
        assertEquals(Folder.PSEUDO_CLASS, rootNodeInfo.getPseudoClass());
    }

    @Test
    void rootNodeParametersTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);
        assertFalse(storage.isWritable(rootNodeInfo.getId()));
        assertTrue(storage.isConsistent(rootNodeInfo.getId()));
    }

    @Test
    void getChildNodesTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);

        // Check the child nodes
        assertEquals(List.of("%2Fcases%2Fn.tst", "%2Fcases%2Fn2.tst"),
            storage.getChildNodes(rootNodeInfo.getId()).stream().map(NodeInfo::getId).collect(Collectors.toList()));
    }

    @Test
    void getChildNodeTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);

        // Case 1
        Optional<NodeInfo> case1 = storage.getChildNode(rootNodeInfo.getId(), "n.tst");
        assertTrue(case1.isPresent());

        // Case 2
        Optional<NodeInfo> case2 = storage.getChildNode(rootNodeInfo.getId(), "n2.tst");
        assertTrue(case2.isPresent());

        // Case 3 (not present)
        Optional<NodeInfo> case3 = storage.getChildNode(rootNodeInfo.getId(), "n3.tst");
        assertTrue(case3.isEmpty());
    }

    @Test
    void childNodeIdTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);
        NodeInfo case1 = storage.getChildNode(rootNodeInfo.getId(), "n.tst").orElseThrow(AssertionError::new);
        assertEquals("%2Fcases%2Fn.tst", case1.getId());
    }

    @Test
    void childNodePseudoClassTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);
        NodeInfo case1 = storage.getChildNode(rootNodeInfo.getId(), "n.tst").orElseThrow(AssertionError::new);
        assertEquals(Case.PSEUDO_CLASS, case1.getPseudoClass());
    }

    @Test
    void childNodeGenericMetadataTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);
        NodeInfo case1 = storage.getChildNode(rootNodeInfo.getId(), "n.tst").orElseThrow(AssertionError::new);
        assertEquals("TEST", case1.getGenericMetadata().getString("format"));
    }

    @Test
    void childNodeDescriptionTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);
        NodeInfo case1 = storage.getChildNode(rootNodeInfo.getId(), "n.tst").orElseThrow(AssertionError::new);
        assertEquals("Test format", case1.getDescription());
    }

    @Test
    void getParentNodeForRootNodeTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);

        // Check parent node for root node
        assertFalse(storage.getParentNode(rootNodeInfo.getId()).isPresent());
    }

    @Test
    void getParentNodeForChildNodeTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);

        // Check parent node for node child
        Optional<NodeInfo> case1 = storage.getChildNode(rootNodeInfo.getId(), "n.tst");
        assertEquals(rootNodeInfo, storage.getParentNode(case1.orElseThrow(AssertionError::new).getId()).orElseThrow(AssertionError::new));
    }

    @Test
    void appStorageDataSourceFromChildNodeTest() {
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists("mem", Folder.PSEUDO_CLASS);
        NodeInfo case1 = storage.getChildNode(rootNodeInfo.getId(), "n.tst").orElseThrow(AssertionError::new);
        DataSource ds = new AppStorageDataSource(storage, case1.getId(), case1.getName());
        assertNotNull(ds);
        assertTrue(ds.isDataExtension("foo"));
    }

    @Test
    void setDescriptionExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.setDescription("any", "any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void setConsistentExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.setConsistent("any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void renameNodeExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.renameNode("any", "any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void updateModificationTimeExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.updateModificationTime("any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void setParentNodeExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.setParentNode("any", "any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void createNodeExceptionTest() {
        NodeGenericMetadata metadata = new NodeGenericMetadata();
        AfsException exception = assertThrows(AfsException.class, () -> storage.createNode("any", "any", "any", "any", 1, metadata));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void deleteNodeExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.deleteNode("any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void writeBinaryDataExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.writeBinaryData("any", "any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void removeDataExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.removeData("any", "any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void createTimeSeriesExceptionTest() {
        TimeSeriesMetadata metadata = new TimeSeriesMetadata("name", TimeSeriesDataType.DOUBLE, InfiniteTimeSeriesIndex.INSTANCE);
        AfsException exception = assertThrows(AfsException.class, () -> storage.createTimeSeries("any", metadata));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void addDoubleTimeSeriesDataExceptionTest() {
        DoubleDataChunk chunk = mock(DoubleDataChunk.class);
        List<DoubleDataChunk> chunks = List.of(chunk);
        AfsException exception = assertThrows(AfsException.class, () -> storage.addDoubleTimeSeriesData("any", 1, "any", chunks));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void addStringTimeSeriesDataExceptionTest() {
        StringDataChunk chunk = mock(StringDataChunk.class);
        List<StringDataChunk> chunks = List.of(chunk);
        AfsException exception = assertThrows(AfsException.class, () -> storage.addStringTimeSeriesData("any", 1, "any", chunks));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void clearTimeSeriesExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.clearTimeSeries("any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void addDependencyExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.addDependency("any", "any", "any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void getDependencies2ArgsExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.getDependencies("any", "any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void getDependencies1ArgExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.getDependencies("any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void getBackwardDependenciesExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.getBackwardDependencies("any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }

    @Test
    void removeDependencyExceptionTest() {
        AfsException exception = assertThrows(AfsException.class, () -> storage.removeDependency("any", "any", "any"));
        assertEquals(METHOD_NOT_IMPLEMENTED, exception.getMessage());
    }
}
