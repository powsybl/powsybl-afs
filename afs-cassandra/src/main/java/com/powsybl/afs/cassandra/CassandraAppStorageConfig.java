/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.powsybl.commons.config.ModuleConfig;
import com.powsybl.commons.config.PlatformConfig;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraAppStorageConfig {

    private static final int DEFAULT_FLUSH_MAXIMUM_CHANGE = 1000;
    private static final long DEFAULT_FLUSH_MAXIMUM_SIZE = Math.round(Math.pow(2, 20));
    private static final int DEFAULT_DOUBLE_QUERY_PARTITION_SIZE = 1000;
    private static final int DEFAULT_STRING_QUERY_PARTITION_SIZE = 1000;
    private static final int DEFAULT_BINARY_DATA_CHUNK_SIZE = (int) Math.round(Math.pow(2, 20));

    private int flushMaximumChange;

    private long flushMaximumSize;

    private int doubleQueryPartitionSize;

    private int stringQueryPartitionSize;

    private int binaryDataChunkSize;

    public static CassandraAppStorageConfig load() {
        return load(PlatformConfig.defaultConfig());
    }

    public static CassandraAppStorageConfig load(PlatformConfig platformConfig) {
        int flushMaximumChange = DEFAULT_FLUSH_MAXIMUM_CHANGE;
        long flushMaximumSize = DEFAULT_FLUSH_MAXIMUM_SIZE;
        int doubleQueryPartitionSize = DEFAULT_DOUBLE_QUERY_PARTITION_SIZE;
        int stringQueryPartitionSize = DEFAULT_STRING_QUERY_PARTITION_SIZE;
        int binaryDataChunkSize = DEFAULT_BINARY_DATA_CHUNK_SIZE;
        ModuleConfig moduleConfig = platformConfig.getOptionalModuleConfig("cassandra-app-storage").orElse(null);
        if (moduleConfig != null) {
            flushMaximumChange = moduleConfig.getIntProperty("flush-maximum-change", DEFAULT_FLUSH_MAXIMUM_CHANGE);
            flushMaximumSize = moduleConfig.getLongProperty("flush-maximum-size", DEFAULT_FLUSH_MAXIMUM_SIZE);
            doubleQueryPartitionSize = moduleConfig.getIntProperty("double-query-partition-size", DEFAULT_DOUBLE_QUERY_PARTITION_SIZE);
            stringQueryPartitionSize = moduleConfig.getIntProperty("string-query-partition-size", DEFAULT_STRING_QUERY_PARTITION_SIZE);
            binaryDataChunkSize = moduleConfig.getIntProperty("binary-data-chunk-size", DEFAULT_BINARY_DATA_CHUNK_SIZE);
        }
        return new CassandraAppStorageConfig(flushMaximumChange, flushMaximumSize, doubleQueryPartitionSize,
                stringQueryPartitionSize, binaryDataChunkSize);
    }

    private static int checkFlushMaximumChange(int flushMaximumChange) {
        if (flushMaximumChange < 1) {
            throw new IllegalArgumentException("Invalid flush maximum change " + flushMaximumChange);
        }
        return flushMaximumChange;
    }

    private static long checkFlushMaximumSize(long flushMaximumSize) {
        if (flushMaximumSize < 1) {
            throw new IllegalArgumentException("Invalid flush maximum size " + flushMaximumSize);
        }
        return flushMaximumSize;
    }

    private static int checkQueryPartitionSize(int queryPartitionSize) {
        if (queryPartitionSize < 1) {
            throw new IllegalArgumentException("Invalid query partition size " + queryPartitionSize);
        }
        return queryPartitionSize;
    }

    private static int checkBinaryDataChunkSize(int binaryDataChunkSize) {
        if (binaryDataChunkSize < 1) {
            throw new IllegalArgumentException("Invalid binary data chunk size " + binaryDataChunkSize);
        }
        return binaryDataChunkSize;
    }

    public CassandraAppStorageConfig() {
        this(DEFAULT_FLUSH_MAXIMUM_CHANGE, DEFAULT_FLUSH_MAXIMUM_SIZE, DEFAULT_DOUBLE_QUERY_PARTITION_SIZE,
                DEFAULT_STRING_QUERY_PARTITION_SIZE, DEFAULT_BINARY_DATA_CHUNK_SIZE);
    }

    public CassandraAppStorageConfig(int flushMaximumChange, long flushMaximumSize, int doubleQueryPartitionSize,
                                     int stringQueryPartitionSize, int binaryDataChunkSize) {
        this.flushMaximumChange = checkFlushMaximumChange(flushMaximumChange);
        this.flushMaximumSize = checkFlushMaximumSize(flushMaximumSize);
        this.doubleQueryPartitionSize = checkQueryPartitionSize(doubleQueryPartitionSize);
        this.stringQueryPartitionSize = checkQueryPartitionSize(stringQueryPartitionSize);
        this.binaryDataChunkSize = checkBinaryDataChunkSize(binaryDataChunkSize);
    }

    public int getFlushMaximumChange() {
        return flushMaximumChange;
    }

    public CassandraAppStorageConfig setFlushMaximumChange(int flushMaximumChange) {
        this.flushMaximumChange = checkFlushMaximumChange(flushMaximumChange);
        return this;
    }

    public long getFlushMaximumSize() {
        return flushMaximumSize;
    }

    public CassandraAppStorageConfig setFlushMaximumSize(long flushMaximumSize) {
        this.flushMaximumSize = checkFlushMaximumSize(flushMaximumSize);
        return this;
    }

    public int getDoubleQueryPartitionSize() {
        return doubleQueryPartitionSize;
    }

    public CassandraAppStorageConfig setDoubleQueryPartitionSize(int doubleQueryPartitionSize) {
        this.doubleQueryPartitionSize = checkQueryPartitionSize(doubleQueryPartitionSize);
        return this;
    }

    public int getStringQueryPartitionSize() {
        return stringQueryPartitionSize;
    }

    public CassandraAppStorageConfig setStringQueryPartitionSize(int stringQueryPartitionSize) {
        this.stringQueryPartitionSize = checkQueryPartitionSize(stringQueryPartitionSize);
        return this;
    }

    public int getBinaryDataChunkSize() {
        return binaryDataChunkSize;
    }

    public CassandraAppStorageConfig setBinaryDataChunkSize(int binaryDataChunkSize) {
        this.binaryDataChunkSize = checkBinaryDataChunkSize(binaryDataChunkSize);
        return this;
    }
}
