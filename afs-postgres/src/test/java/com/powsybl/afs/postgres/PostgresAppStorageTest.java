/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.google.common.collect.Sets;
import com.powsybl.afs.postgres.jpa.NodeDataRepository;
import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.events.TimeSeriesCreated;
import com.powsybl.afs.storage.events.TimeSeriesDataUpdated;
import com.powsybl.timeseries.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Instant;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {PostgresAppStorageTest.class})
@ComponentScan(basePackages = {"com.powsybl.afs.postgres.jpa", "com.powsybl.afs.postgres"})
@EnableAutoConfiguration
@ActiveProfiles({"test"})
public class PostgresAppStorageTest extends AbstractAppStorageTest {
    @Autowired
    private NodeService nodeService;
    @Autowired
    private TimeSeriesService tsService;
    @Autowired
    private NodeDataRepository nodeDataRepository;

    @Override
    protected AppStorage createStorage() {
        return new PostgresAppStorage(nodeService, nodeDataRepository, tsService);
    }

    @Override
    protected void nextDependentTests() throws InterruptedException {
        super.nextDependentTests();
        // 16.2) irregular and compressed
        // TODO move to super if other app storage implements these functions too.
        NodeInfo rootFolderInfo = storage.createRootNodeIfNotExists(storage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        storage.flush();
        NodeInfo testData2Info = storage.createNode(rootFolderInfo.getId(), "data", "data", "", 0, new NodeGenericMetadata());
        storage.flush();
        eventStack.take();
        List<Instant> instants = Arrays.asList(Instant.parse("2015-01-01T00:00:00Z"),
                Instant.parse("2015-01-01T01:00:02Z"),
                Instant.parse("2015-01-01T01:00:05Z"),
                Instant.parse("2015-01-01T01:00:07Z"),
                Instant.parse("2015-01-01T01:00:55Z"),
                Instant.parse("2015-01-01T01:01:55Z"),
                Instant.parse("2015-01-01T01:02:55Z"),
                Instant.parse("2015-01-01T01:03:55Z")
        );
        IrregularTimeSeriesIndex irrIndex = IrregularTimeSeriesIndex.create(instants);
        TimeSeriesMetadata metadata3 = new TimeSeriesMetadata("ts3", TimeSeriesDataType.DOUBLE, Collections.emptyMap(), irrIndex);
        storage.createTimeSeries(testData2Info.getId(), metadata3);
        storage.flush();
        assertEventStack(new TimeSeriesCreated(testData2Info.getId(), "ts3"));

        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(testData2Info.getId(), Sets.newHashSet("ts3"));
        assertEquals(1, metadataList.size());
        assertEquals(irrIndex, metadataList.get(0).getIndex());

        List<DoubleDataChunk> doubleChunks = new ArrayList<>();
        doubleChunks.add(new CompressedDoubleDataChunk(0, 4, new double[]{1.0, 3.0}, new int[]{2, 2}));
        doubleChunks.add(new UncompressedDoubleDataChunk(6, new double[]{5}));

        storage.addDoubleTimeSeriesData(testData2Info.getId(), 0, "ts3", doubleChunks);
        storage.flush();
        assertEventStack(new TimeSeriesDataUpdated(testData2Info.getId(), "ts3"));
        Map<String, List<DoubleDataChunk>> doubleTimeSeriesData3 = storage.getDoubleTimeSeriesData(testData2Info.getId(), Sets.newHashSet("ts3"), 0);
        assertEquals(1, doubleTimeSeriesData3.size());
        assertEquals(Arrays.asList(new CompressedDoubleDataChunk(0, 4, new double[]{1.0, 3.0}, new int[]{2, 2}),
                new UncompressedDoubleDataChunk(6, new double[]{5})),
                doubleTimeSeriesData3.get("ts3"));

        List<StringDataChunk> stringChunks = new ArrayList<>();
        stringChunks.add(new CompressedStringDataChunk(5, 10, new String[]{"a", "b", "c"}, new int[]{3, 3, 4}));
        stringChunks.add(new UncompressedStringDataChunk(20, new String[]{"d", "e"}));
        storage.addStringTimeSeriesData(testData2Info.getId(), 0, "ts4", stringChunks);
        storage.flush();
        assertEventStack(new TimeSeriesDataUpdated(testData2Info.getId(), "ts4"));
        Map<String, List<StringDataChunk>> stringTimeSeriesData4 = storage.getStringTimeSeriesData(testData2Info.getId(), Sets.newHashSet("ts4"), 0);
        assertEquals(1, stringTimeSeriesData4.size());
        assertEquals(Arrays.asList(new CompressedStringDataChunk(5, 10, new String[]{"a", "b", "c"}, new int[]{3, 3, 4}),
                new UncompressedStringDataChunk(20, new String[]{"d", "e"})),
                stringTimeSeriesData4.get("ts4"));
        try {
            storage.getNodeInfo(UUID.randomUUID().toString());
            fail();
        } catch (PostgresAfsException e) {
            // do nothing
        }
    }
}
