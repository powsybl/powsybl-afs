/*
 * Copyright (c) 2019-2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.timeseries.RegularTimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesDataType;
import com.powsybl.timeseries.TimeSeriesMetadata;

import java.util.Collections;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class CassandraLeak {

    public void test(AppStorage storage, CqlSession cassandraSession) {
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "folder");
        NodeInfo nodeInfo = storage.createNode(rootNodeId.getId(), "test1", "folder", "", 0, new NodeGenericMetadata());
        storage.createTimeSeries(nodeInfo.getId(), new TimeSeriesMetadata("ts1", TimeSeriesDataType.DOUBLE, Collections.emptyMap(),
                new RegularTimeSeriesIndex(100000, 100200, 100)));
        storage.flush();
        ResultSet resultSet = cassandraSession
                .execute(selectFrom(CassandraConstants.REGULAR_TIME_SERIES)
                        .countAll()
                        .build());
        Row one = resultSet.one();
        assertNotNull(one);
        assertEquals(1, one.getLong(0));
        storage.deleteNode(nodeInfo.getId());
        storage.flush();
        resultSet = cassandraSession
                .execute(selectFrom(CassandraConstants.REGULAR_TIME_SERIES)
                        .countAll()
                        .build());
        one = resultSet.one();
        assertNotNull(one);
        assertEquals(0, one.getLong(0));
    }
}
