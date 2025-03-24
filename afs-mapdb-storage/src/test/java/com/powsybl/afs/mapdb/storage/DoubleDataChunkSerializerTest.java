/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.mapdb.storage;

import com.powsybl.timeseries.CompressedDoubleDataChunk;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.UncompressedDoubleDataChunk;
import org.junit.jupiter.api.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class DoubleDataChunkSerializerTest {

    private final DoubleDataChunkSerializer serializer = DoubleDataChunkSerializer.INSTANCE;

    @Test
    void uncompressedChunkSerializationTest() throws IOException {
        // Create a chunk
        UncompressedDoubleDataChunk expectedChunk = new UncompressedDoubleDataChunk(5, new double[]{1.0, 2.0, Double.NaN});

        // Serialize the chunk
        byte[] serialized;
        try (DataOutput2 out = new DataOutput2()) {
            serializer.serialize(out, expectedChunk);
            serialized = out.copyBytes();
        }

        // Deserialize the chunk
        DataInput2 in = new DataInput2.ByteArray(serialized);
        DoubleDataChunk deserializedChunk = serializer.deserialize(in, 0);

        // Compare the chunks
        assertInstanceOf(UncompressedDoubleDataChunk.class, deserializedChunk);
        UncompressedDoubleDataChunk result = (UncompressedDoubleDataChunk) deserializedChunk;
        assertArrayEquals(expectedChunk.getValues(), result.getValues(), 0.001);
    }

    @Test
    void compressedChunkSerializationTest() throws IOException {
        // Create a chunk
        CompressedDoubleDataChunk original = new CompressedDoubleDataChunk(10, 10, new double[]{5.0, 6.0}, new int[]{3, 7});

        // Serialize the chunk
        byte[] serialized;
        try (DataOutput2 out = new DataOutput2()) {
            serializer.serialize(out, original);
            serialized = out.copyBytes();
        }

        // Deserialize the chunk
        DataInput2 in = new DataInput2.ByteArray(serialized);
        DoubleDataChunk deserializedChunk = serializer.deserialize(in, 0);

        // Compare the chunks
        assertInstanceOf(CompressedDoubleDataChunk.class, deserializedChunk);
        CompressedDoubleDataChunk result = (CompressedDoubleDataChunk) deserializedChunk;
        assertEquals(original.getUncompressedLength(), result.getUncompressedLength());
        assertArrayEquals(original.getStepValues(), result.getStepValues(), 0.001);
    }

    @Test
    void invalidChunkTypeSerializationTest() throws IOException {
        DoubleDataChunk invalidChunk = mock(DoubleDataChunk.class);
        try (DataOutput2 out = new DataOutput2()) {
            MapDbAfsException exception = assertThrows(MapDbAfsException.class, () -> serializer.serialize(out, invalidChunk));
            assertEquals("Unexpected chunk type", exception.getMessage());
        }
    }

    @Test
    void invalidTypeDeserializationTest() throws IOException {
        try (DataOutput2 out = new DataOutput2()) {
            out.writeInt(MapDbStorageConstants.STORAGE_VERSION);
            out.writeUTF("invalid_type");
            byte[] invalidData = out.copyBytes();

            DataInput2 in = new DataInput2.ByteArray(invalidData);
            MapDbAfsException exception = assertThrows(MapDbAfsException.class, () -> serializer.deserialize(in, 0));
            assertEquals("Unexpected chunk type", exception.getMessage());
        }
    }
}
