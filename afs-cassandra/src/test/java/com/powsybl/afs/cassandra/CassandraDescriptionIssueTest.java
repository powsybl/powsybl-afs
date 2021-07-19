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
public class CassandraDescriptionIssueTest {

    public void test(AppStorage storage) {
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        storage.setConsistent(rootNodeId.getId());
        storage.flush();
        NodeInfo test1NodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "hello", 0, new NodeGenericMetadata());
        assertEquals("hello", test1NodeInfo.getDescription());
        storage.setConsistent(test1NodeInfo.getId());
        storage.flush();
        assertEquals("hello", storage.getNodeInfo(test1NodeInfo.getId()).getDescription());
        assertEquals("hello", storage.getChildNode(rootNodeId.getId(), "test1").get().getDescription());
        storage.setDescription(test1NodeInfo.getId(), "bye");
        storage.flush();
        assertEquals("bye", storage.getNodeInfo(test1NodeInfo.getId()).getDescription());
        assertEquals("bye", storage.getChildNode(rootNodeId.getId(), "test1").get().getDescription());
    }
}
