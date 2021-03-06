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

import static org.junit.Assert.assertEquals;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraDescriptionIssueTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("afs.cql", CassandraConstants.AFS_KEYSPACE), null, 20000L);

    @Test
    public void test() {
        CassandraTestContext context = new CassandraTestContext(cassandraCQLUnit);
        CassandraAppStorage storage = new CassandraAppStorage("test", () -> context,
                new CassandraAppStorageConfig(), new InMemoryEventsBus());
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        storage.setConsistent(rootNodeId.getId());
        storage.flush();
        NodeInfo test1NodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "hello", 0, new NodeGenericMetadata());
        assertEquals("hello", test1NodeInfo.getDescription());
        storage.setConsistent(test1NodeInfo.getId());
        storage.flush();
        assertEquals("hello", storage.getNodeInfo(test1NodeInfo.getId()).getDescription());
        assertEquals("hello", storage.getChildNodes(rootNodeId.getId()).get(0).getDescription());
        storage.setDescription(test1NodeInfo.getId(), "bye");
        storage.flush();
        assertEquals("bye", storage.getNodeInfo(test1NodeInfo.getId()).getDescription());
        assertEquals("bye", storage.getChildNodes(rootNodeId.getId()).get(0).getDescription());
    }
}
