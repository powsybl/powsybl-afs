/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.google.common.collect.Sets;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.timeseries.CompressedStringDataChunk;
import com.powsybl.timeseries.UncompressedDoubleDataChunk;
import com.powsybl.timeseries.UncompressedStringDataChunk;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class TimeSeriesIssueTest {

    public void testEmptyChunks(AppStorage storage) {
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "test");
        storage.addStringTimeSeriesData(rootNodeId.getId(), 0, "ts1", Collections.singletonList(new UncompressedStringDataChunk(0, new String[]{})));
        storage.addDoubleTimeSeriesData(rootNodeId.getId(), 0, "ts2", Collections.singletonList(new UncompressedDoubleDataChunk(0, new double[]{})));
        storage.flush();
        assertTrue(storage.getStringTimeSeriesData(rootNodeId.getId(), Sets.newHashSet("ts1", "ts2"), 0).isEmpty());
    }

    public void testNullString(AppStorage storage) {
        NodeInfo rootNodeId = storage.createRootNodeIfNotExists("test", "test");
        storage.addStringTimeSeriesData(rootNodeId.getId(), 0, "ts1", Arrays.asList(new UncompressedStringDataChunk(0, new String[]{"a", null}),
                new CompressedStringDataChunk(0, 2, new String[]{"a", null}, new int[]{1, 1})));
        storage.flush();
        assertEquals(Collections.singletonMap("ts1", Arrays.asList(new UncompressedStringDataChunk(0, new String[]{"a", ""}),
                new CompressedStringDataChunk(0, 2, new String[]{"a", ""}, new int[]{1, 1}))),
                storage.getStringTimeSeriesData(rootNodeId.getId(), Sets.newHashSet("ts1"), 0));
    }
}
