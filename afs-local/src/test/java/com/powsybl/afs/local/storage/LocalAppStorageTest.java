/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.local.storage;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.Folder;
import com.powsybl.afs.ext.base.Case;
import com.powsybl.afs.ext.base.TestImporter;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.computation.ComputationManager;
import com.powsybl.iidm.network.ImportConfig;
import com.powsybl.iidm.network.ImportersLoaderList;
import com.powsybl.iidm.network.Network;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        ComputationManager computationManager = Mockito.mock(ComputationManager.class);
        Network network = Mockito.mock(Network.class);
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
        assertThrows(AssertionError.class, () -> storage.setConsistent(id));
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
}
