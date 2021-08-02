/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.io.ByteStreams;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import org.cassandraunit.CassandraCQLUnit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.powsybl.afs.cassandra.CassandraConstants.*;
import static org.junit.Assert.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraDataSplitTest {

    public void test(AppStorage storage, CassandraCQLUnit cassandraCQLUnit) throws IOException {

        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        NodeInfo nodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        try (OutputStream os = storage.writeBinaryData(nodeInfo.getId(), "a")) {
            byte[] bytes = "aaaaaaaaaabbbbbbbbbbcccccccccc".getBytes(StandardCharsets.UTF_8);
            // to emulate a BufferedWriter with 1byte buffer size
            for (int i = 0; i < bytes.length; i++) {
                os.write(bytes, i, 1);
            }
        }
        storage.flush();

        InputStream is = storage.readBinaryData(nodeInfo.getId(), "a").orElse(null);
        assertNotNull(is);
        assertEquals("aaaaaaaaaabbbbbbbbbbcccccccccc", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
        is.close();

        try (OutputStream os = storage.writeBinaryData(nodeInfo.getId(), "a")) {
            byte[] bytes = "xaaaaaaaaa".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < bytes.length; i++) {
                os.write(bytes, i, 1);
            }
        }
        storage.flush();

        InputStream is2 = storage.readBinaryData(nodeInfo.getId(), "a").orElse(null);
        assertNotNull(is2);
        assertEquals("xaaaaaaaaa", new String(ByteStreams.toByteArray(is2), StandardCharsets.UTF_8));
        is2.close();

        assertTrue(storage.removeData(nodeInfo.getId(), "a"));
        assertTrue(storage.getDataNames(nodeInfo.getId()).isEmpty());
        assertFalse(storage.readBinaryData(nodeInfo.getId(), "a").isPresent());

        try (OutputStream os = storage.writeBinaryData(nodeInfo.getId(), "a")) {
            byte[] bytes = "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd".getBytes(StandardCharsets.UTF_8);
            os.write(bytes, 0, 5);
            os.write(bytes, 5, 10);
            os.write(bytes, 15, 4);
            os.write(bytes, 19, 3);
            os.write(bytes, 22, 18);
        }
        storage.flush();

        InputStream is3 = storage.readBinaryData(nodeInfo.getId(), "a").orElse(null);
        assertNotNull(is3);
        assertEquals("aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd", new String(ByteStreams.toByteArray(is3), StandardCharsets.UTF_8));
        is3.close();

        ResultSet resultSet = cassandraCQLUnit.getSession().execute(selectFrom(NODE_DATA)
                .column(CHUNKS_COUNT)
                .whereColumn(ID).isEqualTo(literal(UUID.fromString(nodeInfo.getId())))
                .whereColumn(NAME).isEqualTo(literal("a"))
                .build());
        Row firstRow = resultSet.one();
        assertNotNull(firstRow);
        assertEquals(1, firstRow.get(0, Integer.class).intValue());
    }
}
