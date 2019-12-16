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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Ignore
public class CassandraRenameIssueTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("afs.cql", CassandraConstants.AFS_KEYSPACE));

    @Test
    public void test() {
        CassandraTestContext context = new CassandraTestContext(cassandraCQLUnit);
        CassandraAppStorageConfig config = new CassandraAppStorageConfig().setBinaryDataChunkSize(10);
        CassandraAppStorage storage = new CassandraAppStorage("test", () -> context, config, new InMemoryEventsBus());
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        NodeInfo test1NodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        storage.createNode(test1NodeInfo.getId(), "test2", "folder", "", 0, new NodeGenericMetadata());
        storage.createNode(test1NodeInfo.getId(), "test2_bis", "folder", "", 0, new NodeGenericMetadata());
        storage.flush();
        storage.renameNode(test1NodeInfo.getId(), "newtest1");
        storage.flush();
        assertEquals(2, storage.getChildNodes(test1NodeInfo.getId()).size());
        assertEquals("newtest1", storage.getNodeInfo(test1NodeInfo.getId()).getName());
    }
}
