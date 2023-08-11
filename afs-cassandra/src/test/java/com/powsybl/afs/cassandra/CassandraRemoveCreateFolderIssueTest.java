/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraRemoveCreateFolderIssueTest {

    public void test(AppStorage storage) {
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("CassandraRemoveCreateFolderIssueTest", "folder");
        NodeInfo nodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        storage.flush();
        storage.deleteNode(nodeInfo.getId());
        storage.flush();
        NodeInfo nodeInfo2 = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        storage.setConsistent(nodeInfo2.getId());
        storage.flush();
        assertNotEquals(nodeInfo.getId(), nodeInfo2.getId());
        assertEquals(nodeInfo2.getId(), storage.getChildNode(rootNodeId.getId(), "test1").get().getId());
    }
}
