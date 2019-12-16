/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.google.common.io.ByteStreams;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Ignore
public class CassandraDataSplitTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("afs.cql", CassandraConstants.AFS_KEYSPACE));

    @Test
    public void test() throws IOException  {
        CassandraAppStorage storage = new CassandraAppStorage("test", () -> new CassandraTestContext(cassandraCQLUnit),
                new CassandraAppStorageConfig().setBinaryDataChunkSize(10), new InMemoryEventsBus());
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        NodeInfo nodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        try (OutputStream os = storage.writeBinaryData(nodeInfo.getId(), "a")) {
            os.write("aaaaaaaaaabbbbbbbbbbcccccccccc".getBytes(StandardCharsets.UTF_8));
        }
        storage.flush();

        InputStream is = storage.readBinaryData(nodeInfo.getId(), "a").orElse(null);
        assertNotNull(is);
        assertEquals("aaaaaaaaaabbbbbbbbbbcccccccccc", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));

        assertTrue(storage.removeData(nodeInfo.getId(), "a"));
        assertTrue(storage.getDataNames(nodeInfo.getId()).isEmpty());
        assertFalse(storage.readBinaryData(nodeInfo.getId(), "a").isPresent());
    }
}
