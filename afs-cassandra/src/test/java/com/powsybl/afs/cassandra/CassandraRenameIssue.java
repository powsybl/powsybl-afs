/*
 * Copyright (c) 2019-2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class CassandraRenameIssue {

    public void test(AppStorage storage) {

        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        NodeInfo test1NodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        storage.flush();
        NodeInfo test2 = storage.createNode(test1NodeInfo.getId(), "test2", "folder", "", 0, new NodeGenericMetadata());
        storage.setConsistent(test2.getId());
        NodeInfo test2Bis = storage.createNode(test1NodeInfo.getId(), "test2_bis", "folder", "", 0, new NodeGenericMetadata());
        storage.setConsistent(test2Bis.getId());
        storage.flush();
        storage.renameNode(test1NodeInfo.getId(), "newtest1");
        storage.flush();
        assertEquals(2, storage.getChildNodes(test1NodeInfo.getId()).size());
        assertEquals("newtest1", storage.getNodeInfo(test1NodeInfo.getId()).getName());
    }

    public void testRenameChildWithSameName(AppStorage storage) {
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        NodeInfo test1NodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        storage.flush();
        NodeInfo test2Project = storage.createNode(test1NodeInfo.getId(), "test2", "project", "", 0, new NodeGenericMetadata());
        storage.setConsistent(test2Project.getId());
        NodeInfo test2Folder = storage.createNode(test1NodeInfo.getId(), "test2", "folder", "", 0, new NodeGenericMetadata());
        storage.setConsistent(test2Folder.getId());
        storage.flush();
        storage.renameNode(test2Project.getId(), "newTest2");
        storage.flush();
        List<NodeInfo> childNodes = storage.getChildNodes(test1NodeInfo.getId());
        assertEquals(2, childNodes.size());
        Optional<NodeInfo> first = childNodes.stream().filter(nodeInfo -> nodeInfo.getId().equals(test2Project.getId())).findFirst();
        assertTrue(first.isPresent());
        assertEquals("newTest2", first.get().getName());
        Optional<NodeInfo> second = childNodes.stream().filter(nodeInfo -> nodeInfo.getId().equals(test2Folder.getId())).findFirst();
        assertTrue(second.isPresent());
        assertEquals("test2", second.get().getName());
        assertEquals("test2", storage.getNodeInfo(test2Folder.getId()).getName());
        assertEquals("newTest2", storage.getNodeInfo(test2Project.getId()).getName());
    }
}
