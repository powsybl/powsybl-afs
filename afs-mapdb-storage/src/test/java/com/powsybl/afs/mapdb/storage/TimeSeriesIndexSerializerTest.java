/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.mapdb.storage;

import com.powsybl.timeseries.InfiniteTimeSeriesIndex;
import com.powsybl.timeseries.RegularTimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesIndex;
import org.junit.jupiter.api.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class TimeSeriesIndexSerializerTest {

    @Test
    void regularIndexSerializationTest() throws IOException {
        // Initialize an index
        RegularTimeSeriesIndex index = new RegularTimeSeriesIndex(Instant.ofEpochMilli(1672531200000L), Instant.ofEpochMilli(1672617600000L), Duration.ofMillis(3600000L));

        // Serialize the index
        DataOutput2 out = new DataOutput2();
        TimeSeriesIndexSerializer.INSTANCE.serialize(out, index);
        byte[] serialized = out.copyBytes();

        // Deserialize the index
        DataInput2 in = new DataInput2.ByteArray(serialized);
        TimeSeriesIndex deserializedIndex = TimeSeriesIndexSerializer.INSTANCE.deserialize(in, 0);

        // Checks
        assertInstanceOf(RegularTimeSeriesIndex.class, deserializedIndex);
        RegularTimeSeriesIndex deserializedRegularIndex = (RegularTimeSeriesIndex) deserializedIndex;
        assertEquals(index.getStartInstant(), deserializedRegularIndex.getStartInstant());
        assertEquals(index.getEndInstant(), deserializedRegularIndex.getEndInstant());
        assertEquals(index.getTimeStep(), deserializedRegularIndex.getTimeStep());
    }

    @Test
    void deserializeInvalidIndexTypeExceptionTest() throws IOException {
        // Création de données avec un type d'index invalide
        try (DataOutput2 out = new DataOutput2()) {
            out.writeInt(MapDbStorageConstants.STORAGE_VERSION);
            out.writeUTF("invalidType");
            byte[] invalidData = out.copyBytes();

            DataInput2 in = new DataInput2.ByteArray(invalidData);
            MapDbAfsException exception = assertThrows(MapDbAfsException.class, () -> TimeSeriesIndexSerializer.INSTANCE.deserialize(in, 0));
            assertEquals("Index is not a regular time series index", exception.getMessage());
        }
    }

    @Test
    void serializeUnsupportedIndexTypeExceptionTest() throws IOException {
        TimeSeriesIndex index = InfiniteTimeSeriesIndex.INSTANCE;
        try (DataOutput2 out = new DataOutput2()) {
            MapDbAfsException exception = assertThrows(MapDbAfsException.class, () -> TimeSeriesIndexSerializer.INSTANCE.serialize(out, index));
            assertEquals("Index is not a regular time series index", exception.getMessage());
        }
    }
}
