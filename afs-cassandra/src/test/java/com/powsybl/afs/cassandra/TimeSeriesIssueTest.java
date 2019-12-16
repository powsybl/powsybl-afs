/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.google.common.collect.Sets;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.timeseries.*;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.*;
import org.threeten.extra.Interval;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Ignore
public class TimeSeriesIssueTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("afs.cql", CassandraConstants.AFS_KEYSPACE));

    private CassandraAppStorage storage;
    private NodeInfo rootNodeId;

    @Before
    public void setUp() {
        storage = new CassandraAppStorage("test", () -> new CassandraTestContext(cassandraCQLUnit),
                new CassandraAppStorageConfig().setBinaryDataChunkSize(10), new InMemoryEventsBus());
        rootNodeId = storage.createRootNodeIfNotExists("test", "test");
        RegularTimeSeriesIndex index = RegularTimeSeriesIndex.create(Interval.parse("2015-01-01T00:00:00Z/2015-01-01T01:45:00Z"),
                                                                     Duration.ofMinutes(15));
        storage.createTimeSeries(rootNodeId.getId(), new TimeSeriesMetadata("ts1", TimeSeriesDataType.STRING, index));
        storage.createTimeSeries(rootNodeId.getId(), new TimeSeriesMetadata("ts2", TimeSeriesDataType.DOUBLE, index));
    }

    @After
    public void tearDown() {
        storage.close();
    }

    @Test
    public void testEmptyChunks() {
        storage.addStringTimeSeriesData(rootNodeId.getId(), 0, "ts1", Collections.singletonList(new UncompressedStringDataChunk(0, new String[] {})));
        storage.addDoubleTimeSeriesData(rootNodeId.getId(), 0, "ts2", Collections.singletonList(new UncompressedDoubleDataChunk(0, new double[] {})));
        storage.flush();
        assertTrue(storage.getStringTimeSeriesData(rootNodeId.getId(), Sets.newHashSet("ts1", "ts2"), 0).isEmpty());
    }

    @Test
    public void testNullString() {
        storage.addStringTimeSeriesData(rootNodeId.getId(), 0, "ts1", Arrays.asList(new UncompressedStringDataChunk(0, new String[] {"a", null}),
                                                                                    new CompressedStringDataChunk(0, 2, new String[] {"a", null}, new int[] {1, 1})));
        storage.flush();
        assertEquals(Collections.singletonMap("ts1", Arrays.asList(new UncompressedStringDataChunk(0, new String[] {"a", ""}),
                                                                   new CompressedStringDataChunk(0, 2, new String[] {"a", ""}, new int[] {1, 1}))),
                     storage.getStringTimeSeriesData(rootNodeId.getId(), Sets.newHashSet("ts1"), 0));
    }
}
