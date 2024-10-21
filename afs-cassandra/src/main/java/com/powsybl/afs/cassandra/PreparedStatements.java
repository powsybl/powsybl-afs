/*
 * Copyright (c) 2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.powsybl.afs.cassandra.CassandraConstants.*;
import static com.powsybl.afs.cassandra.CassandraConstants.VALUES;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class PreparedStatements {
    private final PreparedStatement createTimeSeriesPreparedStmt;
    private final PreparedStatement insertTimeSeriesDataChunksPreparedStmt;
    private final PreparedStatement insertDoubleTimeSeriesDataCompressedChunksPreparedStmt;
    private final PreparedStatement insertDoubleTimeSeriesDataUncompressedChunksPreparedStmt;
    private final PreparedStatement insertStringTimeSeriesDataCompressedChunksPreparedStmt;
    private final PreparedStatement insertStringTimeSeriesDataUncompressedChunksPreparedStmt;

    PreparedStatements(CassandraAppStorage storage) {
        createTimeSeriesPreparedStmt = storage.getSession().prepare(
            insertInto(REGULAR_TIME_SERIES)
                .value(ID, bindMarker())
                .value(TIME_SERIES_NAME, bindMarker())
                .value(DATA_TYPE, bindMarker())
                .value(TIME_SERIES_TAGS, bindMarker())
                .value(START, bindMarker())
                .value(END, bindMarker())
                .value(SPACING, bindMarker()).build());

        insertTimeSeriesDataChunksPreparedStmt = storage.getSession().prepare(
            insertInto(TIME_SERIES_DATA_CHUNK_TYPES)
                .value(ID, bindMarker())
                .value(TIME_SERIES_NAME, bindMarker())
                .value(VERSION, bindMarker())
                .value(CHUNK_ID, bindMarker())
                .value(CHUNK_TYPE, bindMarker())
                .build());

        insertDoubleTimeSeriesDataCompressedChunksPreparedStmt = storage.getSession().prepare(
            insertInto(DOUBLE_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                .value(ID, bindMarker())
                .value(TIME_SERIES_NAME, bindMarker())
                .value(VERSION, bindMarker())
                .value(CHUNK_ID, bindMarker())
                .value(OFFSET, bindMarker())
                .value(UNCOMPRESSED_LENGTH, bindMarker())
                .value(STEP_VALUES, bindMarker())
                .value(STEP_LENGTHS, bindMarker())
                .build());

        insertDoubleTimeSeriesDataUncompressedChunksPreparedStmt = storage.getSession().prepare(
            insertInto(DOUBLE_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                .value(ID, bindMarker())
                .value(TIME_SERIES_NAME, bindMarker())
                .value(VERSION, bindMarker())
                .value(CHUNK_ID, bindMarker())
                .value(OFFSET, bindMarker())
                .value(VALUES, bindMarker())
                .build());

        insertStringTimeSeriesDataCompressedChunksPreparedStmt = storage.getSession().prepare(
            insertInto(STRING_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                .value(ID, bindMarker())
                .value(TIME_SERIES_NAME, bindMarker())
                .value(VERSION, bindMarker())
                .value(CHUNK_ID, bindMarker())
                .value(OFFSET, bindMarker())
                .value(UNCOMPRESSED_LENGTH, bindMarker())
                .value(STEP_VALUES, bindMarker())
                .value(STEP_LENGTHS, bindMarker())
                .build());

        insertStringTimeSeriesDataUncompressedChunksPreparedStmt = storage.getSession().prepare(
            insertInto(STRING_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                .value(ID, bindMarker())
                .value(TIME_SERIES_NAME, bindMarker())
                .value(VERSION, bindMarker())
                .value(CHUNK_ID, bindMarker())
                .value(OFFSET, bindMarker())
                .value(VALUES, bindMarker())
                .build());
    }

    PreparedStatement getCreateTimeSeriesPreparedStmt() {
        return createTimeSeriesPreparedStmt;
    }

    PreparedStatement getInsertTimeSeriesDataChunksPreparedStmt() {
        return insertTimeSeriesDataChunksPreparedStmt;
    }

    PreparedStatement getInsertDoubleTimeSeriesDataCompressedChunksPreparedStmt() {
        return insertDoubleTimeSeriesDataCompressedChunksPreparedStmt;
    }

    PreparedStatement getInsertDoubleTimeSeriesDataUncompressedChunksPreparedStmt() {
        return insertDoubleTimeSeriesDataUncompressedChunksPreparedStmt;
    }

    PreparedStatement getInsertStringTimeSeriesDataCompressedChunksPreparedStmt() {
        return insertStringTimeSeriesDataCompressedChunksPreparedStmt;
    }

    PreparedStatement getInsertStringTimeSeriesDataUncompressedChunksPreparedStmt() {
        return insertStringTimeSeriesDataUncompressedChunksPreparedStmt;
    }
}
