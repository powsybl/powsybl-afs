/*
 * Copyright (c) 2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import com.powsybl.afs.storage.events.NodeDataUpdated;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.powsybl.afs.cassandra.CassandraConstants.*;
import static com.powsybl.afs.storage.AbstractAppStorage.APPSTORAGE_NODE_TOPIC;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class BinaryDataOutputStream extends OutputStream {

    private final CassandraAppStorage storage;
    private final UUID nodeUuid;
    private final String name;
    private final CassandraAppStorageConfig config;
    private ByteArrayOutputStream buffer;
    private long count = 0;
    private int chunkNum = 0;
    private GZIPOutputStream gzos;

    BinaryDataOutputStream(CassandraAppStorage storage, CassandraAppStorageConfig config, UUID nodeUuid, String name) {
        this.storage = storage;
        this.nodeUuid = Objects.requireNonNull(nodeUuid);
        this.name = Objects.requireNonNull(name);
        this.config = config;
        this.buffer = new ByteArrayOutputStream(this.config.getBinaryDataChunkSize());
        try {
            gzos = new GZIPOutputStream(buffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void execute() {
        try {
            gzos.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // at first write clear previous data to prevent just overlapping on a potential previous data of greater length
        if (chunkNum == 0) {
            storage.removeData(nodeUuid.toString(), name);
        }

        storage.getSession().execute(insertInto(NODE_DATA)
            .value(ID, literal(nodeUuid))
            .value(NAME, literal(name))
            .value(CHUNK_NUM, literal(chunkNum++))
            .value(CHUNKS_COUNT, literal(chunkNum))
            .value(CHUNK, literal(ByteBuffer.wrap(buffer.toByteArray())))
            .build());
        buffer = new ByteArrayOutputStream(config.getBinaryDataChunkSize());
        try {
            gzos = new GZIPOutputStream(buffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void executeIfNecessary() {
        if (count >= config.getBinaryDataChunkSize()) {
            execute();
            count = 0;
        }
    }

    @Override
    public void write(int b) throws IOException {
        gzos.write(b);
        count++;
        executeIfNecessary();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len + count > config.getBinaryDataChunkSize()) {
            int chunkOffset = off;
            long writtenLen = 0;
            while (writtenLen < len) {
                long chunkLen = Math.min(config.getBinaryDataChunkSize() - count, len - writtenLen);
                gzos.write(b, chunkOffset, (int) chunkLen);
                count += chunkLen;
                writtenLen += chunkLen;
                chunkOffset += (int) chunkLen;
                executeIfNecessary();
            }
        } else {
            gzos.write(b, off, len);
            count += len;
            executeIfNecessary();
        }
    }

    @Override
    public void close() {
        if (chunkNum == 0 || count > 0) { // create  at least on chunk even empty
            execute();
        }

        // update data names
        storage.getSession().execute(insertInto(NODE_DATA_NAMES)
            .value(ID, literal(nodeUuid))
            .value(NAME, literal(name))
            .build());

        storage.pushEvent(new NodeDataUpdated(nodeUuid.toString(), name), APPSTORAGE_NODE_TOPIC);
    }
}
