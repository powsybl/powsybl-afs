/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraRemoveCreateFolderIssueTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("afs.cql", CassandraConstants.AFS_KEYSPACE));

    @Test
    public void test() {
        CassandraTestContext context = new CassandraTestContext(cassandraCQLUnit);
        CassandraAppStorageConfig config = new CassandraAppStorageConfig().setBinaryDataChunkSize(10);
        CassandraAppStorage storage = new CassandraAppStorage("test", () -> context, config, new InMemoryEventsBus());
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        NodeInfo nodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        storage.flush();
        storage.deleteNode(nodeInfo.getId());
        storage.flush();
        NodeInfo nodeInfo2 = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        storage.setConsistent(nodeInfo2.getId());
        storage.flush();
        assertNotEquals(nodeInfo.getId(), nodeInfo2.getId());
        assertEquals(Collections.singletonList(nodeInfo2.getId()), storage.getChildNodes(rootNodeId.getId()).stream().map(NodeInfo::getId).collect(Collectors.toList()));
    }
}
