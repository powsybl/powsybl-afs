/*
 * Copyright (c) 2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.UUID;
import java.util.function.IntSupplier;
import java.util.zip.GZIPInputStream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.powsybl.afs.cassandra.CassandraConstants.*;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class BinaryDataInputStream extends InputStream {

    private final CassandraAppStorage storage;
    private final UUID nodeUuid;
    private final String name;
    private ByteArrayInputStream buffer;
    private GZIPInputStream gzis;
    private int chunkNum = 1;

    BinaryDataInputStream(CassandraAppStorage storage, UUID nodeUuid, String name, Row firstRow) {
        this.storage = storage;
        this.nodeUuid = Objects.requireNonNull(nodeUuid);
        this.name = Objects.requireNonNull(name);
        buffer = new ByteArrayInputStream(firstRow.getByteBuffer(0).array());
        try {
            gzis = new GZIPInputStream(buffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int read() {
        return read(() -> {
            try {
                return gzis.read();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return read(() -> {
            try {
                return gzis.read(b, off, len);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private int read(IntSupplier supplier) {
        int c;
        c = supplier.getAsInt();
        if (c == -1) {
            // try to get next chunk
            ResultSet resultSet = storage.getSession().execute(selectFrom(NODE_DATA)
                .column(CHUNK)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .whereColumn(CHUNK_NUM).isEqualTo(literal(chunkNum))
                .build());
            Row row = resultSet.one();
            if (row != null) {
                try {
                    gzis.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                byte[] array = row.getByteBuffer(0).array();
                buffer = new ByteArrayInputStream(array);
                try {
                    gzis = new GZIPInputStream(buffer);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                c = supplier.getAsInt();
                chunkNum++;
            }
        }
        return c;
    }
}
