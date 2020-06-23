/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.timeseries.RegularTimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesDataType;
import com.powsybl.timeseries.TimeSeriesMetadata;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraLeakTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("afs.cql", CassandraConstants.AFS_KEYSPACE), null, 20000L);

    @Test
    public void test() {
        CassandraAppStorage storage = new CassandraAppStorage("test", () -> new CassandraTestContext(cassandraCQLUnit),
                new CassandraAppStorageConfig(), new InMemoryEventsBus());
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        NodeInfo nodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        storage.createTimeSeries(nodeInfo.getId(), new TimeSeriesMetadata("ts1", TimeSeriesDataType.DOUBLE, Collections.emptyMap(),
                new RegularTimeSeriesIndex(100000, 100200, 100)));
        storage.flush();
        ResultSet resultSet = cassandraCQLUnit.getSession().execute(select().countAll().from(CassandraConstants.REGULAR_TIME_SERIES));
        Row one = resultSet.one();
        assertNotNull(one);
        assertEquals(1, one.getLong(0));
        storage.deleteNode(nodeInfo.getId());
        storage.flush();
        resultSet = cassandraCQLUnit.getSession().execute(select().countAll().from(CassandraConstants.REGULAR_TIME_SERIES));
        one = resultSet.one();
        assertNotNull(one);
        assertEquals(0, one.getLong(0));
    }
}
