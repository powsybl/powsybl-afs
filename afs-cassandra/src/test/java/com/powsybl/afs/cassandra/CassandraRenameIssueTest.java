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

import static org.junit.Assert.assertEquals;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraRenameIssueTest {

    public void test(AppStorage storage) {
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("CassandraRenameIssueTest", "folder");
        NodeInfo newNode = storage.createNode(rootNodeId.getId(), "CassandraRenameIssueTest_test2", "folder", "", 0, new NodeGenericMetadata());
        storage.flush();
        assertEquals("CassandraRenameIssueTest_test2", storage.getNodeInfo(newNode.getId()).getName());

        storage.renameNode(newNode.getId(), "newtest1");
        storage.flush();
        assertEquals("newtest1", storage.getNodeInfo(newNode.getId()).getName());
    }
}
