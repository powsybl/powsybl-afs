/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.mapdb.storage;

import com.powsybl.timeseries.CompressedStringDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.UncompressedStringDataChunk;
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
class StringDataChunkSerializerTest {

    private final StringDataChunkSerializer serializer = StringDataChunkSerializer.INSTANCE;

    @Test
    void uncompressedChunkSerializationTest() throws IOException {
        // Create a chunk
        UncompressedStringDataChunk expectedChunk = new UncompressedStringDataChunk(5, new String[]{"a", "b", ""});

        // Serialize the chunk
        byte[] serialized;
        try (DataOutput2 out = new DataOutput2()) {
            serializer.serialize(out, expectedChunk);
            serialized = out.copyBytes();
        }

        // Deserialize the chunk
        DataInput2 in = new DataInput2.ByteArray(serialized);
        StringDataChunk deserializedChunk = serializer.deserialize(in, 0);

        // Compare the chunks
        assertInstanceOf(UncompressedStringDataChunk.class, deserializedChunk);
        UncompressedStringDataChunk result = (UncompressedStringDataChunk) deserializedChunk;
        assertArrayEquals(expectedChunk.getValues(), result.getValues());
    }

    @Test
    void compressedChunkSerializationTest() throws IOException {
        // Create a chunk
        CompressedStringDataChunk original = new CompressedStringDataChunk(10, 10, new String[]{"a", "b"}, new int[]{3, 7});

        // Serialize the chunk
        byte[] serialized;
        try (DataOutput2 out = new DataOutput2()) {
            serializer.serialize(out, original);
            serialized = out.copyBytes();
        }

        // Deserialize the chunk
        DataInput2 in = new DataInput2.ByteArray(serialized);
        StringDataChunk deserializedChunk = serializer.deserialize(in, 0);

        // Compare the chunks
        assertInstanceOf(CompressedStringDataChunk.class, deserializedChunk);
        CompressedStringDataChunk result = (CompressedStringDataChunk) deserializedChunk;
        assertEquals(original.getUncompressedLength(), result.getUncompressedLength());
        assertArrayEquals(original.getStepValues(), result.getStepValues());
    }

    @Test
    void invalidChunkTypeSerializationTest() throws IOException {
        StringDataChunk invalidChunk = mock(StringDataChunk.class);
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
