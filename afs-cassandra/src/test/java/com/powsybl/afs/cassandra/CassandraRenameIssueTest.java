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
}
