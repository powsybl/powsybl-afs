/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.Session;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.powsybl.afs.cassandra.CassandraConstants.*;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
final class CompressOnceBinaryDataOutputStream extends AbstractCassandraOutputStream {

    private final ByteArrayOutputStream baos;
    private final GZIPOutputStream gzos;

    CompressOnceBinaryDataOutputStream(UUID nodeUuid, String name, int chunkSize, Session session) {
        super(nodeUuid, name, chunkSize, session);
        baos = new ByteArrayOutputStream();
        try {
            gzos = new GZIPOutputStream(baos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void write(int b) throws IOException {
        gzos.write(b);
        executeIfNecessary();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        gzos.write(b, off, len);
        executeIfNecessary();
    }

    private void executeIfNecessary() {
        final int length = baos.size();
        if (length > (chunkNum + 1) * chunkSize) {
            writeToDb();
        }
    }

    private void writeToDb() {
        final byte[] bytes = baos.toByteArray();
        for (int i = chunkNum; i < bytes.length / chunkSize; i++) {
            int offset = i * chunkSize;
            session.execute(insertInto(NODE_DATA)
                    .value(ID, nodeUuid)
                    .value(NAME, name)
                    .value(CHUNK_NUM, chunkNum++)
                    .value(CHUNKS_COUNT, chunkNum)
                    .value(CHUNK, ByteBuffer.wrap(bytes, offset, chunkSize)));
        }
    }

    @Override
    public void close() throws IOException {
        gzos.close();
        writeToDb();
        final byte[] bytes = baos.toByteArray();
        final int remainder = bytes.length % chunkSize;
        if (remainder != 0) {
            session.execute(insertInto(NODE_DATA)
                    .value(ID, nodeUuid)
                    .value(NAME, name)
                    .value(CHUNK_NUM, chunkNum++)
                    .value(CHUNKS_COUNT, chunkNum)
                    .value(CHUNK, ByteBuffer.wrap(bytes, bytes.length - remainder, remainder)));
        }
    }
}
