/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.buffer.*;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import com.powsybl.afs.storage.events.*;
import com.powsybl.timeseries.*;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.powsybl.afs.cassandra.CassandraConstants.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraAppStorage extends AbstractAppStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAppStorage.class);

    private static final String BROKEN_DEPENDENCY = "Broken dependency";

    public static final String REF_NOT_FOUND = "REFERENCE_NOT_FOUND";

    public static final String ORPHAN_NODE = "ORPHAN_NODE";

    public static final String ORPHAN_DATA = "ORPHAN_DATA";

    private final String fileSystemName;

    private final Supplier<CassandraContext> contextSupplier;

    private final CassandraAppStorageConfig config;

    private final class PreparedStatements {

        private final PreparedStatement createTimeSeriesPreparedStmt;

        private final PreparedStatement insertTimeSeriesDataChunksPreparedStmt;

        private final PreparedStatement insertDoubleTimeSeriesDataCompressedChunksPreparedStmt;

        private final PreparedStatement insertDoubleTimeSeriesDataUncompressedChunksPreparedStmt;

        private final PreparedStatement insertStringTimeSeriesDataCompressedChunksPreparedStmt;

        private final PreparedStatement insertStringTimeSeriesDataUncompressedChunksPreparedStmt;

        private PreparedStatements() {
            createTimeSeriesPreparedStmt = getSession().prepare(
                    insertInto(REGULAR_TIME_SERIES)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(DATA_TYPE, bindMarker())
                    .value(TIME_SERIES_TAGS, bindMarker())
                    .value(START, bindMarker())
                    .value(END, bindMarker())
                    .value(SPACING, bindMarker()).build());

            insertTimeSeriesDataChunksPreparedStmt = getSession().prepare(
                    insertInto(TIME_SERIES_DATA_CHUNK_TYPES)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(VERSION, bindMarker())
                    .value(CHUNK_ID, bindMarker())
                    .value(CHUNK_TYPE, bindMarker())
                    .build());

            insertDoubleTimeSeriesDataCompressedChunksPreparedStmt = getSession().prepare(
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

            insertDoubleTimeSeriesDataUncompressedChunksPreparedStmt = getSession().prepare(
                    insertInto(DOUBLE_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(VERSION, bindMarker())
                    .value(CHUNK_ID, bindMarker())
                    .value(OFFSET, bindMarker())
                    .value(VALUES, bindMarker())
                    .build());

            insertStringTimeSeriesDataCompressedChunksPreparedStmt = getSession().prepare(
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

            insertStringTimeSeriesDataUncompressedChunksPreparedStmt = getSession().prepare(
                    insertInto(STRING_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(VERSION, bindMarker())
                    .value(CHUNK_ID, bindMarker())
                    .value(OFFSET, bindMarker())
                    .value(VALUES, bindMarker())
                    .build());
        }
    }

    private final Supplier<PreparedStatements> preparedStatementsSupplier;

    private enum TimeSeriesChunkType {
        DOUBLE_UNCOMPRESSED,
        DOUBLE_COMPRESSED,
        STRING_UNCOMPRESSED,
        STRING_COMPRESSED
    }

    private static class TimeSeriesWritingContext {
        int createdTimeSeriesCount = 0;
        int insertedChunkCount = 0;
    }

    private final StorageChangeFlusher changeFlusher = new StorageChangeFlusher() {

        private void flush(TimeSeriesCreation creation, List<Statement> statements, TimeSeriesWritingContext writingContext) {
            if (creation.getMetadata().getIndex() instanceof RegularTimeSeriesIndex) {
                RegularTimeSeriesIndex index = (RegularTimeSeriesIndex) creation.getMetadata().getIndex();
                statements.add(preparedStatementsSupplier.get().createTimeSeriesPreparedStmt
                        .bind()
                        .setUuid(ID, checkNodeId(creation.getNodeId()))
                        .setString(TIME_SERIES_NAME, creation.getMetadata().getName())
                        .setString(DATA_TYPE, creation.getMetadata().getDataType().name())
                        .setMap(TIME_SERIES_TAGS, creation.getMetadata().getTags(), String.class, String.class)
                        .setInstant(START, Instant.ofEpochMilli(index.getStartTime()))
                        .setInstant(END, Instant.ofEpochMilli(index.getEndTime()))
                        .setLong(SPACING, index.getSpacing()));
            } else {
                throw new AssertionError();
            }

            writingContext.createdTimeSeriesCount++;
        }

        private void flush(DoubleTimeSeriesChunksAddition addition, List<Statement> statements, TimeSeriesWritingContext writingContext) {
            UUID nodeUuid = checkNodeId(addition.getNodeId());
            int version = addition.getVersion();
            String timeSeriesName = addition.getTimeSeriesName();
            List<DoubleDataChunk> chunks = addition.getChunks();

            for (DoubleDataChunk chunk : chunks) {
                UUID chunkId = Uuids.timeBased();

                // skip empty chunks
                if (chunk.getLength() == 0) {
                    LOGGER.warn("Empty chunk for time series {} version {} of node {}", timeSeriesName, version, nodeUuid);
                    continue;
                }

                statements.add(preparedStatementsSupplier.get().insertTimeSeriesDataChunksPreparedStmt
                        .bind()
                        .setUuid(ID, nodeUuid)
                        .setString(TIME_SERIES_NAME, timeSeriesName)
                        .setInt(VERSION, version)
                        .setUuid(CHUNK_ID, chunkId)
                        .setInt(CHUNK_TYPE, chunk.isCompressed() ? TimeSeriesChunkType.DOUBLE_COMPRESSED.ordinal()
                                : TimeSeriesChunkType.DOUBLE_UNCOMPRESSED.ordinal()));

                if (chunk.isCompressed()) {
                    statements.add(preparedStatementsSupplier.get().insertDoubleTimeSeriesDataCompressedChunksPreparedStmt
                            .bind()
                            .setUuid(ID, nodeUuid)
                            .setString(TIME_SERIES_NAME, timeSeriesName)
                            .setInt(VERSION, version)
                            .setUuid(CHUNK_ID, chunkId)
                            .setInt(OFFSET, chunk.getOffset())
                            .setInt(UNCOMPRESSED_LENGTH, chunk.getLength())
                            .setList(STEP_VALUES, Arrays.stream(((CompressedDoubleDataChunk) chunk).getStepValues()).boxed().collect(Collectors.toList()), Double.class)
                            .setList(STEP_LENGTHS, Arrays.stream(((CompressedDoubleDataChunk) chunk).getStepLengths()).boxed().collect(Collectors.toList()), Integer.class));

                } else {
                    statements.add(preparedStatementsSupplier.get().insertDoubleTimeSeriesDataUncompressedChunksPreparedStmt
                            .bind()
                            .setUuid(ID, nodeUuid)
                            .setString(TIME_SERIES_NAME, timeSeriesName)
                            .setInt(VERSION, version)
                            .setUuid(CHUNK_ID, chunkId)
                            .setInt(OFFSET, chunk.getOffset())
                            .setList(VALUES, Arrays.stream(((UncompressedDoubleDataChunk) chunk).getValues()).boxed().collect(Collectors.toList()), Double.class));
                }

                writingContext.insertedChunkCount++;
            }
        }

        /**
         * Cassandra does not support null values in collection, so in order to avoid the following error, we replace
         * null strings by empty strings.
         *
         * java.lang.NullPointerException: Collection elements cannot be null
         *
         *     at com.datastax.driver.core.TypeCodec$AbstractCollectionCodec.serialize(TypeCodec.java:1767)
         *     at com.datastax.driver.core.TypeCodec$AbstractCollectionCodec.serialize(TypeCodec.java:1749)
         *     at com.datastax.driver.core.AbstractData.setList(AbstractData.java:358)
         *     at com.datastax.driver.core.BoundStatement.setList(BoundStatement.java:654)
         *     at com.rte_france.powsybl.afs.cassandra.CassandraAppStorage$1.flush(CassandraAppStorage.java:179)
         */
        private List<String> fixNullValues(String[] values) {
            Objects.requireNonNull(values);
            return Arrays.stream(values).map(v -> v != null ? v : "").collect(Collectors.toList());
        }

        private void flush(StringTimeSeriesChunksAddition addition, List<Statement> statements, TimeSeriesWritingContext writingContext) {
            UUID nodeUuid = checkNodeId(addition.getNodeId());
            int version = addition.getVersion();
            String timeSeriesName = addition.getTimeSeriesName();
            List<StringDataChunk> chunks = addition.getChunks();

            for (StringDataChunk chunk : chunks) {
                UUID chunkId = Uuids.timeBased();

                // skip empty chunks
                if (chunk.getLength() == 0) {
                    LOGGER.warn("Empty chunk for time series {} version {} of node {}", timeSeriesName, version, nodeUuid);
                    continue;
                }

                statements.add(preparedStatementsSupplier.get().insertTimeSeriesDataChunksPreparedStmt
                        .bind()
                        .setUuid(ID, nodeUuid)
                        .setString(TIME_SERIES_NAME, timeSeriesName)
                        .setInt(VERSION, version)
                        .setUuid(CHUNK_ID, chunkId)
                        .setInt(CHUNK_TYPE, chunk.isCompressed() ? TimeSeriesChunkType.STRING_COMPRESSED.ordinal()
                                : TimeSeriesChunkType.STRING_UNCOMPRESSED.ordinal()));

                if (chunk.isCompressed()) {
                    statements.add(preparedStatementsSupplier.get().insertStringTimeSeriesDataCompressedChunksPreparedStmt
                            .bind()
                            .setUuid(ID, nodeUuid)
                            .setString(TIME_SERIES_NAME, timeSeriesName)
                            .setInt(VERSION, version)
                            .setUuid(CHUNK_ID, chunkId)
                            .setInt(OFFSET, chunk.getOffset())
                            .setInt(UNCOMPRESSED_LENGTH, chunk.getLength())
                            .setList(STEP_VALUES, fixNullValues(((CompressedStringDataChunk) chunk).getStepValues()), String.class)
                            .setList(STEP_LENGTHS, Arrays.stream(((CompressedStringDataChunk) chunk).getStepLengths())
                                    .boxed()
                                    .collect(Collectors.toList()), Integer.class));

                } else {
                    statements.add(preparedStatementsSupplier.get().insertStringTimeSeriesDataUncompressedChunksPreparedStmt
                            .bind()
                            .setUuid(ID, nodeUuid)
                            .setString(TIME_SERIES_NAME, timeSeriesName)
                            .setInt(VERSION, version)
                            .setUuid(CHUNK_ID, chunkId)
                            .setInt(OFFSET, chunk.getOffset())
                            .setList(VALUES, fixNullValues(((UncompressedStringDataChunk) chunk).getValues()), String.class));

                }

                writingContext.insertedChunkCount++;
            }
        }

        @Override
        public void flush(StorageChangeSet changeSet) {
            Stopwatch watch = Stopwatch.createStarted();

            List<Statement> statements = new ArrayList<>();

            TimeSeriesWritingContext writingContext = new TimeSeriesWritingContext();

            for (StorageChange change : changeSet.getChanges()) {
                switch (change.getType()) {
                    case TIME_SERIES_CREATION:
                        flush((TimeSeriesCreation) change, statements, writingContext);
                        break;
                    case DOUBLE_TIME_SERIES_CHUNKS_ADDITION:
                        flush((DoubleTimeSeriesChunksAddition) change, statements, writingContext);
                        break;
                    case STRING_TIME_SERIES_CHUNKS_ADDITION:
                        flush((StringTimeSeriesChunksAddition) change, statements, writingContext);
                        break;
                    default:
                        throw new AssertionError();
                }
            }

            // write statements
            int i = 0;
            try {
                for (; i < statements.size(); i++) {
                    getSession().execute(statements.get(i));
                }
            } catch (Exception e) {
                LOGGER.error("Failed to execute statement {}. The subsequent buffered changes in {} will be ignored.", statements.get(i), changeSet);
                throw e;
            }

            watch.stop();

            if (writingContext.createdTimeSeriesCount > 0 || writingContext.insertedChunkCount > 0) {
                LOGGER.info("{} times series created and {} data chunks inserted in {} ms",
                        writingContext.createdTimeSeriesCount, writingContext.insertedChunkCount, watch.elapsed(TimeUnit.MILLISECONDS));
            }
        }
    };

    private final StorageChangeBuffer changeBuffer;

    public CassandraAppStorage(String fileSystemName, Supplier<CassandraContext> contextSupplier,
                               CassandraAppStorageConfig config, EventsBus eventsBus) {
        this.eventsBus = eventsBus;
        this.fileSystemName = Objects.requireNonNull(fileSystemName);
        this.contextSupplier = Suppliers.memoize(Objects.requireNonNull(contextSupplier));
        this.config = Objects.requireNonNull(config);

        // WARNING: Cassandra cannot mutate more that 16Mo per query!
        changeBuffer = new StorageChangeBuffer(changeFlusher, config.getFlushMaximumChange(), config.getFlushMaximumSize());

        // prepared statement
        preparedStatementsSupplier = Suppliers.memoize(PreparedStatements::new);
    }

    @Override
    public String getFileSystemName() {
        return fileSystemName;
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    private static CassandraAfsException createNodeNotFoundException(UUID nodeId) {
        return new CassandraAfsException("Node '" + nodeId + "' not found");
    }

    private CqlSession getSession() {
        return Objects.requireNonNull(contextSupplier.get()).getSession();
    }

    @Override
    public NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(nodePseudoClass);

        SimpleStatement insertQuery =
                insertInto(ROOT_NODE)
                        .value(ROOT_ID, now())
                        .value(FS_NAME, literal(fileSystemName))
                        .ifNotExists()
                        .build();

        // check if root node with same name has already been created
        ResultSet insertResultSet = getSession().execute(insertQuery);
        boolean rootCreated = insertResultSet.wasApplied();

        SimpleStatement selectQuery = selectFrom(ROOT_NODE)
                .column(ROOT_ID)
                .whereColumn(FS_NAME).isEqualTo(literal(fileSystemName))
                .build();
        ResultSet selectResultSet = getSession().execute(selectQuery);
        UUID rootNodeUuid = selectResultSet.one().getUuid(0);

        NodeInfo rootNodeInfo;
        if (rootCreated) {
            BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);
            rootNodeInfo = createNode(rootNodeUuid, null, name, nodePseudoClass, "", 0, new NodeGenericMetadata(), batchStatementBuilder);
            getSession().execute(batchStatementBuilder.build());
            setConsistent(rootNodeInfo.getId());
        } else {
            rootNodeInfo = getNodeInfo(rootNodeUuid);
        }

        return rootNodeInfo;
    }

    private String getNodeName(UUID nodeUuid) {
        Objects.requireNonNull(nodeUuid);
        SimpleStatement selectQuery = selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .distinct()
                .column(NAME)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build();
        ResultSet resultSet = getSession().execute(selectQuery);
        Row row = resultSet.one();
        if (row == null) {
            throw createNodeNotFoundException(nodeUuid);
        }
        return row.getString(0);
    }

    @Override
    public boolean isWritable(String nodeId) {
        return true;
    }

    private static boolean isConsistentBackwardCompatible(Row row, int i) {
        return row.isNull(i) || row.getBoolean(i);
    }

    @Override
    public boolean isConsistent(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(nodeUuid);
        SimpleStatement selectQuery = selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .distinct()
                .column(CONSISTENT)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build();
        ResultSet resultSet = getSession().execute(selectQuery);
        Row row = resultSet.one();
        if (row == null) {
            throw createNodeNotFoundException(nodeUuid);
        }
        return isConsistentBackwardCompatible(row, 0);
    }

    private NodeInfo createNode(UUID parentNodeUuid, String name, String nodePseudoClass, String description,
                                int version, NodeGenericMetadata genericMetadata, BatchStatementBuilder batchStatementBuilder) {
        return createNode(Uuids.timeBased(), parentNodeUuid, name, nodePseudoClass, description, version, genericMetadata, batchStatementBuilder);
    }

    private NodeInfo createNode(UUID nodeUuid, UUID parentNodeUuid, String name, String nodePseudoClass,
                                String description, int version, NodeGenericMetadata genericMetadata,
                                BatchStatementBuilder batchStatementBuilder) {
        long creationTime = ZonedDateTime.now().toInstant().toEpochMilli();
        batchStatementBuilder.addStatement(insertInto(CHILDREN_BY_NAME_AND_CLASS)
                .value(ID, literal(nodeUuid))
                .value(NAME, literal(name))
                .value(PARENT_ID, literal(parentNodeUuid))
                .value(PSEUDO_CLASS, literal(nodePseudoClass))
                .value(DESCRIPTION, literal(description))
                .value(CONSISTENT, literal(false))
                .value(CREATION_DATE, literal(Instant.ofEpochMilli(creationTime)))
                .value(MODIFICATION_DATE, literal(Instant.ofEpochMilli(creationTime)))
                .values(addAllMetadata(genericMetadata))
                .build());

        if (parentNodeUuid != null) {
            batchStatementBuilder.addStatement(insertInto(CHILDREN_BY_NAME_AND_CLASS)
                    .value(ID, literal(parentNodeUuid))
                    .value(CHILD_NAME, literal(name))
                    .value(CHILD_ID, literal(nodeUuid))
                    .value(CHILD_PSEUDO_CLASS, literal(nodePseudoClass))
                    .value(CHILD_DESCRIPTION, literal(description))
                    .value(CHILD_CONSISTENT, literal(false))
                    .value(CHILD_CREATION_DATE, literal(Instant.ofEpochMilli(creationTime)))
                    .value(CHILD_MODIFICATION_DATE, literal(Instant.ofEpochMilli(creationTime)))
                    .value(CHILD_VERSION, literal(version))
                    .values(addAllChildMetadata(genericMetadata))
                    .build());
        }
        pushEvent(new NodeCreated(nodeUuid.toString(), parentNodeUuid != null ? parentNodeUuid.toString() : null), APPSTORAGE_NODE_TOPIC);
        return new NodeInfo(nodeUuid.toString(), name, nodePseudoClass, description, creationTime, creationTime, version, genericMetadata);
    }

    @Override
    public NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, int version,
                               NodeGenericMetadata genericMetadata) {
        UUID parentNodeUuid = checkNodeId(parentNodeId);
        Objects.requireNonNull(name);
        Objects.requireNonNull(nodePseudoClass);

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);
        NodeInfo nodeInfo = createNode(parentNodeUuid, name, nodePseudoClass, description, version, genericMetadata, batchStatementBuilder);
        getSession().execute(batchStatementBuilder.build());
        return nodeInfo;
    }

    @Override
    public void setMetadata(String nodeId, NodeGenericMetadata genericMetadata) {
        UUID nodeUuid = checkNodeId(nodeId);
        NodeGenericMetadata newMetadata = genericMetadata != null ? genericMetadata : new NodeGenericMetadata();

        // flush buffer to keep change order
        changeBuffer.flush();

        //Update updateCurrantLine = .where(ID).isEqualTo(literal(nodeUuid));
        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);
        batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(MD, literal(newMetadata.getDoubles()))
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(MT, literal(newMetadata.getStrings()))
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(MI, literal(newMetadata.getInts()))
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(MB, literal(newMetadata.getBooleans()))
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());

        UUID parentNodeUuid = getParentNodeUuid(nodeUuid);
        if (parentNodeUuid != null) {
            NodeInfo nodeInfo = getNodeInfo(nodeId);
            batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                    .setColumn(CMD, literal(newMetadata.getDoubles()))
                    .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                    .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                    .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                    .build());
            batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                    .setColumn(CMT, literal(newMetadata.getStrings()))
                    .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                    .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                    .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                    .build());
            batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                    .setColumn(CMI, literal(newMetadata.getInts()))
                    .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                    .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                    .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                    .build());
            batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                    .setColumn(CMB, literal(newMetadata.getBooleans()))
                    .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                    .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                    .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                    .build());
        }

        getSession().execute(batchStatementBuilder.build());
        pushEvent(new NodeMetadataUpdated(nodeId, newMetadata), APPSTORAGE_NODE_TOPIC);
    }

    @Override
    public void renameNode(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Impossible to rename node '" + nodeId + "' with an empty name");
        }

        // flush buffer to keep change order
        changeBuffer.flush();

        NodeInfo nodeInfo = getNodeInfo(nodeId);
        getParentNode(nodeId).ifPresent(parentNode -> {
            UUID parentNodeUuid = checkNodeId(parentNode.getId());

            // need to remove and re-insert row because child_name is part of partition key
            getSession().execute(deleteFrom(CHILDREN_BY_NAME_AND_CLASS)
                    .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                    .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                    .build());

            getSession().execute(insertInto(CHILDREN_BY_NAME_AND_CLASS)
                    .value(ID, literal(parentNodeUuid))
                    .value(CHILD_NAME, literal(name))
                    .value(CHILD_ID, literal(nodeUuid))
                    .value(CHILD_PSEUDO_CLASS, literal(nodeInfo.getPseudoClass()))
                    .value(CHILD_DESCRIPTION, literal(nodeInfo.getDescription()))
                    .value(CHILD_CREATION_DATE, literal(Instant.ofEpochMilli(nodeInfo.getCreationTime())))
                    .value(CHILD_MODIFICATION_DATE, literal(Instant.ofEpochMilli(nodeInfo.getModificationTime())))
                    .value(CHILD_VERSION, literal(nodeInfo.getVersion()))
                    .values(addAllMetadata(nodeInfo))
                    .build());

            getSession().execute(update(CHILDREN_BY_NAME_AND_CLASS)
                    .setColumn(NAME, literal(name))
                    .whereColumn(ID).isEqualTo(literal(nodeUuid))
                    .build());
        });
        pushEvent(new NodeNameUpdated(nodeId, name), APPSTORAGE_NODE_TOPIC);
    }

    private static Map<String, Term> addAllMetadata(NodeInfo nodeInfo) {
        return addAllMetadata(nodeInfo.getGenericMetadata());
    }

    private static Map<String, Term> addAllChildMetadata(NodeInfo nodeInfo) {
        return addAllChildMetadata(nodeInfo.getGenericMetadata());
    }

    private static Map<String, Term> addAllMetadata(NodeGenericMetadata genericMetadata) {
        return Arrays.stream(NodeMetadataGetter.values())
            .collect(Collectors.toUnmodifiableMap(NodeMetadataGetter::symbol, getter -> literal(getter.apply(genericMetadata))));
    }

    private static Map<String, Term> addAllChildMetadata(NodeGenericMetadata genericMetadata) {
        return Arrays.stream(NodeMetadataGetter.values())
            .collect(Collectors.toUnmodifiableMap(NodeMetadataGetter::childSymbol, getter -> literal(getter.apply(genericMetadata))));
    }

    private static UUID checkNodeId(String nodeId) {
        Objects.requireNonNull(nodeId);
        return UUID.fromString(nodeId);
    }

    @Override
    public NodeInfo getNodeInfo(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        return getNodeInfo(nodeUuid);
    }

    private NodeInfo getNodeInfo(UUID nodeUuid) {
        Objects.requireNonNull(nodeUuid);
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .distinct()
                .columns(NAME, PSEUDO_CLASS, DESCRIPTION, CREATION_DATE, MODIFICATION_DATE, VERSION, MT, MD, MI, MB)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        Row row = resultSet.one();
        if (row == null) {
            throw createNodeNotFoundException(nodeUuid);
        }
        return new NodeInfo(nodeUuid.toString(),
                row.getString(0),
                row.getString(1),
                row.getString(2),
                row.getInstant(3).toEpochMilli(),
                row.getInstant(4).toEpochMilli(),
                row.getInt(5),
                new NodeGenericMetadata(row.getMap(6, String.class, String.class),
                        row.getMap(7, String.class, Double.class),
                        row.getMap(8, String.class, Integer.class),
                        row.getMap(9, String.class, Boolean.class)));
    }

    private void setAttribute(String nodeId, String attributeName, String childAttributeName, Object newValue) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(newValue);

        // flush buffer to keep change order
        changeBuffer.flush();

        UUID parentNodeId = getParentNodeUuid(nodeUuid);
        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);
        batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(attributeName, literal(newValue))
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        if (parentNodeId != null) {
            NodeInfo nodeInfo = getNodeInfo(nodeId);
            batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                    .setColumn(childAttributeName, literal(newValue))
                    .whereColumn(ID).isEqualTo(literal(parentNodeId))
                    .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                    .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                    .build());
        }
        getSession().execute(batchStatementBuilder.build());
    }

    @Override
    public void setDescription(String nodeId, String description) {
        setAttribute(nodeId, DESCRIPTION, CHILD_DESCRIPTION, description);
        pushEvent(new NodeDescriptionUpdated(nodeId, description), APPSTORAGE_NODE_TOPIC);
    }

    @Override
    public void setConsistent(String nodeId) {
        // flush buffer to keep change order
        changeBuffer.flush();
        setAttribute(nodeId, CONSISTENT, CHILD_CONSISTENT, true);
        pushEvent(new NodeConsistent(nodeId), APPSTORAGE_NODE_TOPIC);
    }

    @Override
    public void updateModificationTime(String nodeId) {
        long modificationTime = ZonedDateTime.now().toInstant().toEpochMilli();
        setAttribute(nodeId, MODIFICATION_DATE, CHILD_MODIFICATION_DATE, modificationTime);
    }

    private List<UUID> getChildNodeUuids(UUID nodeUuid) {
        Objects.requireNonNull(nodeUuid);
        List<UUID> childNodeUuids = new ArrayList<>();
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .columns(CHILD_ID, CHILD_CONSISTENT)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        for (Row row : resultSet) {
            UUID uuid = row.getUuid(0);
            if (uuid != null && isConsistentBackwardCompatible(row, 1)) {
                childNodeUuids.add(uuid);
            }
        }
        return childNodeUuids;
    }

    @Override
    public List<NodeInfo> getChildNodes(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        List<NodeInfo> childNodesInfo = new ArrayList<>();
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .columns(CHILD_NAME, CHILD_PSEUDO_CLASS, CHILD_ID, CHILD_DESCRIPTION,
                        CHILD_CREATION_DATE, CHILD_MODIFICATION_DATE, CHILD_VERSION,
                        CMT, CMD, CMI, CMB, CHILD_CONSISTENT)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        for (Row row : resultSet) {
            UUID uuid = row.getUuid(2);
            if (uuid != null && isConsistentBackwardCompatible(row, 11)) {
                NodeInfo nodeInfo = new NodeInfo(uuid.toString(),
                        row.getString(0),
                        row.getString(1),
                        row.getString(3),
                        row.getInstant(4).toEpochMilli(),
                        row.getInstant(5).toEpochMilli(),
                        row.getInt(6),
                        new NodeGenericMetadata(row.getMap(7, String.class, String.class),
                                row.getMap(8, String.class, Double.class),
                                row.getMap(9, String.class, Integer.class),
                                row.getMap(10, String.class, Boolean.class)));
                childNodesInfo.add(nodeInfo);
            }
        }
        return childNodesInfo;
    }

    @Override
    public List<NodeInfo> getInconsistentNodes() {
        List<NodeInfo> childNodesInfo = new ArrayList<>();
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .distinct()
                .columns(NAME, PSEUDO_CLASS, ID, DESCRIPTION,
                        CREATION_DATE, MODIFICATION_DATE, VERSION,
                        MT, MD, MI, MB, CONSISTENT)
                .build());

        for (Row row : resultSet) {
            UUID uuid = row.getUuid(2);
            if (uuid != null && !isConsistentBackwardCompatible(row, 11)) {
                NodeInfo nodeInfo = new NodeInfo(uuid.toString(),
                        row.getString(0),
                        row.getString(1),
                        row.getString(3),
                        row.getInstant(4).toEpochMilli(),
                        row.getInstant(5).toEpochMilli(),
                        row.getInt(6),
                        new NodeGenericMetadata(row.getMap(7, String.class, String.class),
                                row.getMap(8, String.class, Double.class),
                                row.getMap(9, String.class, Integer.class),
                                row.getMap(10, String.class, Boolean.class)));
                childNodesInfo.add(nodeInfo);
            }
        }
        return childNodesInfo;
    }

    @Override
    public Optional<NodeInfo> getChildNode(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .columns(CHILD_ID, CHILD_PSEUDO_CLASS, CHILD_DESCRIPTION,
                        CHILD_CREATION_DATE, CHILD_MODIFICATION_DATE, CHILD_VERSION,
                        CMT, CMD, CMI, CMB, CHILD_CONSISTENT)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(CHILD_NAME).isEqualTo(literal(name))
                .build());
        Row row = resultSet.one();
        if (row == null || !isConsistentBackwardCompatible(row, 10)) {
            return Optional.empty();
        }
        return Optional.of(new NodeInfo(row.getUuid(0).toString(),
                name,
                row.getString(1),
                row.getString(2),
                row.getInstant(3).toEpochMilli(),
                row.getInstant(4).toEpochMilli(),
                row.getInt(5),
                new NodeGenericMetadata(row.getMap(6, String.class, String.class),
                        row.getMap(7, String.class, Double.class),
                        row.getMap(8, String.class, Integer.class),
                        row.getMap(9, String.class, Boolean.class))));
    }

    private UUID getParentNodeUuid(UUID nodeUuid) {
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .distinct()
                .column(PARENT_ID)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        Row row = resultSet.one();
        if (row == null) {
            throw createNodeNotFoundException(nodeUuid);
        }
        return row.getUuid(0);
    }

    @Override
    public Optional<NodeInfo> getParentNode(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        UUID parentNodeUuid = getParentNodeUuid(nodeUuid);
        return Optional.ofNullable(parentNodeUuid).map(this::getNodeInfo);
    }

    @Override
    public void setParentNode(String nodeId, String newParentNodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        UUID newParentNodeUuid = checkNodeId(newParentNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        NodeInfo nodeInfo = getNodeInfo(nodeId);
        UUID currentParentId = getParentNodeUuid(nodeUuid);
        if (currentParentId == null) {
            throw new AfsStorageException("Cannot change parent of root folder");
        }

        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);
        batchStatementBuilder.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(PARENT_ID, literal(newParentNodeUuid))
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        batchStatementBuilder.addStatement(deleteFrom(CHILDREN_BY_NAME_AND_CLASS)
                .whereColumn(ID).isEqualTo(literal(currentParentId))
                .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                .build());
        batchStatementBuilder.addStatement(insertInto(CHILDREN_BY_NAME_AND_CLASS)
                .value(ID, literal(newParentNodeUuid))
                .value(CHILD_NAME, literal(nodeInfo.getName()))
                .value(CHILD_PSEUDO_CLASS, literal(nodeInfo.getPseudoClass()))
                .value(CHILD_ID, literal(nodeUuid))
                .value(CHILD_DESCRIPTION, literal(nodeInfo.getDescription()))
                .value(CHILD_CREATION_DATE, literal(Instant.ofEpochMilli(nodeInfo.getCreationTime())))
                .value(CHILD_MODIFICATION_DATE, literal(Instant.ofEpochMilli(nodeInfo.getModificationTime())))
                .value(CHILD_VERSION, literal(nodeInfo.getVersion()))
                .values(addAllChildMetadata(nodeInfo))
                .build());
        getSession().execute(batchStatementBuilder.build());

        pushEvent(new ParentChanged(nodeId, currentParentId.toString(), newParentNodeId), APPSTORAGE_NODE_TOPIC);
    }

    @Override
    public String deleteNode(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        UUID parentNodeUuid = deleteNode(nodeUuid);
        return parentNodeUuid != null ? parentNodeUuid.toString() : null;
    }

    private UUID deleteNode(UUID nodeUuid) {
        Objects.requireNonNull(nodeUuid);

        // flush buffer to keep change order
        changeBuffer.flush();

        // recursively delete children
        for (UUID childNodeUuid : getChildNodeUuids(nodeUuid)) {
            deleteNode(childNodeUuid);
        }

        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);

        // children
        batchStatementBuilder.addStatement(deleteFrom(CHILDREN_BY_NAME_AND_CLASS)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        UUID parentNodeUuid = getParentNodeUuid(nodeUuid);
        batchStatementBuilder.addStatement(deleteFrom(CHILDREN_BY_NAME_AND_CLASS)
                .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                .whereColumn(CHILD_NAME).isEqualTo(literal(getNodeName(nodeUuid)))
                .build());

        // data
        removeAllData(nodeUuid, batchStatementBuilder);

        // time series
        clearTimeSeries(nodeUuid, batchStatementBuilder);

        // dependencies
        Map<String, UUID> dependencies = getDependencyInfo(nodeUuid);
        Map<String, List<UUID>> backwardDependencies = new HashMap<>();

        ResultSet resultSet = getSession().execute(selectFrom(BACKWARD_DEPENDENCIES)
                .columns(NAME, FROM_ID)
                .whereColumn(TO_ID).isEqualTo(literal(nodeUuid))
                .build());

        for (Row row : resultSet) {
            String name = row.getString(0);
            UUID otherNodeUuid = row.getUuid(1);
            List<UUID> uuids = backwardDependencies.computeIfAbsent(name, depName -> new ArrayList<>());
            uuids.add(otherNodeUuid);
            backwardDependencies.put(name, uuids);
            batchStatementBuilder.addStatement(deleteFrom(DEPENDENCIES)
                    .whereColumn(FROM_ID).isEqualTo(literal(otherNodeUuid))
                    .whereColumn(NAME).isEqualTo(literal(name))
                    .whereColumn(TO_ID).isEqualTo(literal(nodeUuid))
                    .build());
        }

        batchStatementBuilder.addStatement(deleteFrom(DEPENDENCIES)
                .whereColumn(FROM_ID)
                .isEqualTo(literal(nodeUuid))
                .build());

        List<Term> listDependencies = dependencies.values().stream()
                .map(dep -> literal(dep))
                .collect(Collectors.toList());

        if (!listDependencies.isEmpty()) {
            batchStatementBuilder.addStatement(deleteFrom(BACKWARD_DEPENDENCIES)
                    .whereColumn(TO_ID).in(listDependencies)
                    .build());
        }

        getSession().execute(batchStatementBuilder.build());

        backwardDependencies.entrySet().stream().flatMap(dep -> dep.getValue().stream().map(depUuid -> Pair.of(dep.getKey(), depUuid))).forEach(dep -> {
            pushEvent(new DependencyRemoved(dep.getValue().toString(), dep.getKey()), APPSTORAGE_DEPENDENCY_TOPIC);
            pushEvent(new BackwardDependencyRemoved(nodeUuid.toString(), dep.getKey()), APPSTORAGE_DEPENDENCY_TOPIC);
        });
        dependencies.forEach((key, value) -> {
            pushEvent(new DependencyRemoved(nodeUuid.toString(), key), APPSTORAGE_DEPENDENCY_TOPIC);
            pushEvent(new BackwardDependencyRemoved(value.toString(), key), APPSTORAGE_DEPENDENCY_TOPIC);
        });

        pushEvent(new NodeRemoved(nodeUuid.toString(), String.valueOf(parentNodeUuid)), APPSTORAGE_NODE_TOPIC);
        return parentNodeUuid;
    }

    private final class BinaryDataInputStream extends InputStream {

        private final UUID nodeUuid;

        private final String name;

        private ByteArrayInputStream buffer;

        private GZIPInputStream gzis;

        private int chunkNum = 1;

        private BinaryDataInputStream(UUID nodeUuid, String name, Row firstRow) {
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
                ResultSet resultSet = getSession().execute(selectFrom(NODE_DATA)
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

    @Override
    public Optional<InputStream> readBinaryData(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);

        // get first chunk
        ResultSet resultSet = getSession().execute(selectFrom(NODE_DATA)
                .column(CHUNK)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .whereColumn(CHUNK_NUM).isEqualTo(literal(0))
                .build());
        Row firstRow = resultSet.one();
        if (firstRow == null) {
            return Optional.empty();
        }

        return Optional.of(new BinaryDataInputStream(nodeUuid, name, firstRow));
    }

    private final class BinaryDataOutputStream extends OutputStream {

        private final UUID nodeUuid;

        private final String name;

        private ByteArrayOutputStream buffer = new ByteArrayOutputStream(config.getBinaryDataChunkSize());

        private long count = 0;

        private int chunkNum = 0;

        private GZIPOutputStream gzos;

        private BinaryDataOutputStream(UUID nodeUuid, String name) {
            this.nodeUuid = Objects.requireNonNull(nodeUuid);
            this.name = Objects.requireNonNull(name);
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
                removeData(nodeUuid.toString(), name);
            }

            getSession().execute(insertInto(NODE_DATA)
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
                    chunkOffset += chunkLen;
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
            getSession().execute(insertInto(NODE_DATA_NAMES)
                    .value(ID, literal(nodeUuid))
                    .value(NAME, literal(name))
                    .build());

            pushEvent(new NodeDataUpdated(nodeUuid.toString(), name), APPSTORAGE_NODE_TOPIC);
        }
    }

    @Override
    public OutputStream writeBinaryData(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        // flush buffer to keep change order
        changeBuffer.flush();
        return new BinaryDataOutputStream(nodeUuid, name);
    }

    @Override
    public boolean dataExists(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        ResultSet resultSet = getSession().execute(selectFrom(NODE_DATA_NAMES)
                .countAll()
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .build());
        Row row = resultSet.one();
        return row.getLong(0) > 0;
    }

    private Set<String> getDataNames(UUID nodeUuid) {
        ResultSet resultSet = getSession().execute(selectFrom(NODE_DATA_NAMES)
                .column(NAME)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        Set<String> dataNames = new HashSet<>();
        for (Row row : resultSet) {
            dataNames.add(row.getString(0));
        }
        return dataNames;
    }

    @Override
    public Set<String> getDataNames(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        return getDataNames(nodeUuid);
    }

    private void removeData(UUID nodeUuid, String name, BatchStatementBuilder batchStatementBuilder) {
        batchStatementBuilder.addStatement(deleteFrom(NODE_DATA)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .build());
        batchStatementBuilder.addStatement(deleteFrom(NODE_DATA_NAMES)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .build());
        pushEvent(new NodeDataRemoved(nodeUuid.toString(), name), APPSTORAGE_NODE_TOPIC);
    }

    private void removeAllData(UUID nodeUuid, BatchStatementBuilder batchStatementBuilder) {
        for (String dataName : getDataNames(nodeUuid)) {
            removeData(nodeUuid, dataName, batchStatementBuilder);
        }
    }

    @Override
    public boolean removeData(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);

        // get chunk num list
        List<Integer> chunks = new ArrayList<>(1);
        ResultSet resultSet = getSession().execute(selectFrom(NODE_DATA)
                .column(CHUNKS_COUNT)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .build());
        for (Row row : resultSet) {
            chunks.add(row.getInt(0));
        }
        if (chunks.isEmpty()) {
            return false;
        }

        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
        removeData(nodeUuid, name, batchStatementBuilder);
        getSession().execute(batchStatementBuilder.build());

        return true;
    }

    @Override
    public void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        changeBuffer.createTimeSeries(nodeId, metadata);
        pushEvent(new TimeSeriesCreated(nodeId, metadata.getName()),
                String.valueOf(APPSTORAGE_TIMESERIES_TOPIC));
    }

    @Override
    public Set<String> getTimeSeriesNames(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Set<String> timeSeriesNames = new TreeSet<>();
        ResultSet resultSet = getSession().execute(selectFrom(REGULAR_TIME_SERIES)
                .column(TIME_SERIES_NAME)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        for (Row row : resultSet) {
            timeSeriesNames.add(row.getString(0));
        }
        return timeSeriesNames;
    }

    @Override
    public boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(timeSeriesName);
        ResultSet resultSet = getSession().execute(selectFrom(REGULAR_TIME_SERIES)
                .countAll()
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                .build());
        Row row = resultSet.one();
        return row.getLong(0) > 0;
    }

    @Override
    public List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(timeSeriesNames);
        if (timeSeriesNames.isEmpty()) {
            throw new IllegalArgumentException("Empty time series name list");
        }
        List<TimeSeriesMetadata> timeSeries = new ArrayList<>();

        List<Term> timeSeriesNamesList = timeSeriesNames.stream()
                .map(QueryBuilder::literal)
                .collect(Collectors.toList());
        ResultSet resultSet = getSession().execute(selectFrom(REGULAR_TIME_SERIES)
                .columns(TIME_SERIES_NAME, DATA_TYPE, TIME_SERIES_TAGS, START, END, SPACING)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(TIME_SERIES_NAME).in(timeSeriesNamesList)
                .build());
        for (Row row : resultSet) {
            timeSeries.add(new TimeSeriesMetadata(row.getString(0),
                    TimeSeriesDataType.valueOf(row.getString(1)),
                    row.getMap(2, String.class, String.class),
                    new RegularTimeSeriesIndex(row.getInstant(3).toEpochMilli(),
                            row.getInstant(4).toEpochMilli(),
                            row.getLong(5))));
        }
        return timeSeries;
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Set<Integer> versions = new TreeSet<>();
        ResultSet resultSet = getSession().execute(selectFrom(TIME_SERIES_DATA_CHUNK_TYPES)
                .column(VERSION)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        for (Row row : resultSet) {
            versions.add(row.getInt(0));
        }
        return versions;
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(timeSeriesName);
        Set<Integer> versions = new TreeSet<>();
        ResultSet resultSet = getSession().execute(selectFrom(TIME_SERIES_DATA_CHUNK_TYPES)
                .column(VERSION)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                .build());
        for (Row row : resultSet) {
            versions.add(row.getInt(0));
        }
        return versions;
    }

    @Override
    public Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        UUID nodeUuid = checkNodeId(nodeId);
        TimeSeriesVersions.check(version);

        // get time series data, both uncompressed and compressed chunks
        Map<String, List<DoubleDataChunk>> timeSeriesData = new HashMap<>();

        for (List<String> timeSeriesNamesPartition : Lists.partition(new ArrayList<>(timeSeriesNames), config.getDoubleQueryPartitionSize())) {
            ResultSet resultSet = getSession().execute(selectFrom(DOUBLE_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                    .columns(TIME_SERIES_NAME, OFFSET, VALUES)
                    .whereColumn(ID).isEqualTo(literal(nodeUuid))
                    .whereColumn(TIME_SERIES_NAME).in(timeSeriesNamesPartition.stream().map(QueryBuilder::literal).collect(Collectors.toList()))
                    .whereColumn(VERSION).isEqualTo(literal(version))
                    .build());
            for (Row row : resultSet) {
                String name = row.getString(0);
                int offset = row.getInt(1);
                List<Double> values = row.getList(2, Double.class);
                timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new UncompressedDoubleDataChunk(offset, values.stream().mapToDouble(Double::valueOf).toArray()));
            }

            resultSet = getSession().execute(selectFrom(DOUBLE_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                    .columns(TIME_SERIES_NAME, OFFSET, UNCOMPRESSED_LENGTH, STEP_VALUES, STEP_LENGTHS)
                    .whereColumn(ID).isEqualTo(literal(nodeUuid))
                    .whereColumn(TIME_SERIES_NAME).in(timeSeriesNamesPartition.stream().map(QueryBuilder::literal).collect(Collectors.toList()))
                    .whereColumn(VERSION).isEqualTo(literal(version))
                    .build());
            for (Row row : resultSet) {
                String name = row.getString(0);
                int offset = row.getInt(1);
                int length = row.getInt(2);
                List<Double> stepValues = row.getList(3, Double.class);
                List<Integer> stepLengths = row.getList(4, Integer.class);
                timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new CompressedDoubleDataChunk(offset, length,
                                stepValues.stream().mapToDouble(Double::valueOf).toArray(),
                                stepLengths.stream().mapToInt(Integer::valueOf).toArray()));
            }
        }
        return timeSeriesData;
    }

    @Override
    public Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        UUID nodeUuid = checkNodeId(nodeId);
        TimeSeriesVersions.check(version);

        // get time series data, both uncompressed and compressed chunks
        Map<String, List<StringDataChunk>> timeSeriesData = new HashMap<>();

        for (List<String> timeSeriesNamesPartition : Lists.partition(new ArrayList<>(timeSeriesNames), config.getStringQueryPartitionSize())) {
            ResultSet resultSet = getSession().execute(selectFrom(STRING_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                    .columns(TIME_SERIES_NAME, OFFSET, VALUES)
                    .whereColumn(ID).isEqualTo(literal(nodeUuid))
                    .whereColumn(TIME_SERIES_NAME).in(timeSeriesNamesPartition.stream().map(QueryBuilder::literal).collect(Collectors.toList()))
                    .whereColumn(VERSION).isEqualTo(literal(version))
                    .build());
            for (Row row : resultSet) {
                String name = row.getString(0);
                int offset = row.getInt(1);
                List<String> values = row.getList(2, String.class);
                timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new UncompressedStringDataChunk(offset, values.toArray(new String[values.size()])));
            }

            resultSet = getSession().execute(selectFrom(STRING_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                    .columns(TIME_SERIES_NAME, OFFSET, UNCOMPRESSED_LENGTH, STEP_VALUES, STEP_LENGTHS)
                    .whereColumn(ID).isEqualTo(literal(nodeUuid))
                    .whereColumn(TIME_SERIES_NAME).in(timeSeriesNamesPartition.stream().map(QueryBuilder::literal).collect(Collectors.toList()))
                    .whereColumn(VERSION).isEqualTo(literal(version))
                    .build());
            for (Row row : resultSet) {
                String name = row.getString(0);
                int offset = row.getInt(1);
                int length = row.getInt(2);
                List<String> stepValues = row.getList(3, String.class);
                List<Integer> stepLengths = row.getList(4, Integer.class);
                timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new CompressedStringDataChunk(offset, length,
                                stepValues.toArray(new String[stepValues.size()]),
                                stepLengths.stream().mapToInt(Integer::valueOf).toArray()));
            }
        }

        return timeSeriesData;
    }

    @Override
    public void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {
        changeBuffer.addDoubleTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        pushEvent(new TimeSeriesDataUpdated(nodeId, timeSeriesName), APPSTORAGE_TIMESERIES_TOPIC);
    }

    @Override
    public void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {
        changeBuffer.addStringTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        pushEvent(new TimeSeriesDataUpdated(nodeId, timeSeriesName), APPSTORAGE_TIMESERIES_TOPIC);
    }

    private void clearTimeSeries(UUID nodeUuid, BatchStatementBuilder batchStatementBuilder) {
        batchStatementBuilder.addStatement(deleteFrom(REGULAR_TIME_SERIES)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        ResultSet resultSet = getSession().execute(selectFrom(TIME_SERIES_DATA_CHUNK_TYPES)
                .columns(TIME_SERIES_NAME, VERSION, CHUNK_TYPE)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        for (Row row : resultSet) {
            String timeSeriesName = row.getString(0);
            int version = row.getInt(1);
            TimeSeriesChunkType chunkType = TimeSeriesChunkType.values()[row.getInt(2)];
            switch (chunkType) {
                case DOUBLE_UNCOMPRESSED:
                    batchStatementBuilder.addStatement(deleteFrom(DOUBLE_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                            .whereColumn(ID).isEqualTo(literal(nodeUuid)).whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                            .whereColumn(VERSION).isEqualTo(literal(version))
                            .build());
                    break;
                case DOUBLE_COMPRESSED:
                    batchStatementBuilder.addStatement(deleteFrom(DOUBLE_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                            .whereColumn(ID).isEqualTo(literal(nodeUuid)).whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                            .whereColumn(VERSION).isEqualTo(literal(version))
                            .build());
                    break;
                case STRING_UNCOMPRESSED:
                    batchStatementBuilder.addStatement(deleteFrom(STRING_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                            .whereColumn(ID).isEqualTo(literal(nodeUuid)).whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                            .whereColumn(VERSION).isEqualTo(literal(version))
                            .build());
                    break;
                case STRING_COMPRESSED:
                    batchStatementBuilder.addStatement(deleteFrom(STRING_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                            .whereColumn(ID).isEqualTo(literal(nodeUuid)).whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                            .whereColumn(VERSION).isEqualTo(literal(version))
                            .build());
                    break;
                default:
                    throw new AssertionError("Unknown chunk type " + chunkType);
            }
        }
        batchStatementBuilder.addStatement(deleteFrom(TIME_SERIES_DATA_CHUNK_TYPES)
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
    }

    @Override
    public void clearTimeSeries(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
        clearTimeSeries(nodeUuid, batchStatementBuilder);
        getSession().execute(batchStatementBuilder.build());
        pushEvent(new TimeSeriesCleared(nodeUuid.toString()), APPSTORAGE_TIMESERIES_TOPIC);
    }

    @Override
    public void addDependency(String nodeId, String name, String toNodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        UUID toNodeUuid = checkNodeId(toNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);

        batchStatementBuilder.addStatement(insertInto(DEPENDENCIES)
                .value(FROM_ID, literal(nodeUuid))
                .value(NAME, literal(name))
                .value(TO_ID, literal(toNodeUuid))
                .value("dep_id", literal(Uuids.timeBased()))
                .build());
        batchStatementBuilder.addStatement(insertInto(BACKWARD_DEPENDENCIES)
                .value(TO_ID, literal(toNodeUuid))
                .value(NAME, literal(name))
                .value(FROM_ID, literal(nodeUuid))
                .build());
        getSession().execute(batchStatementBuilder.build());
        pushEvent(new DependencyAdded(nodeId, name),
                String.valueOf(APPSTORAGE_DEPENDENCY_TOPIC));
        pushEvent(new BackwardDependencyAdded(toNodeId, name),
                String.valueOf(APPSTORAGE_DEPENDENCY_TOPIC));
    }

    private Map<String, UUID> getDependencyInfo(UUID nodeUuid) {
        Objects.requireNonNull(nodeUuid);
        Map<String, UUID> dependencies = new HashMap<>();
        ResultSet resultSet = getSession().execute(selectFrom(DEPENDENCIES)
                .columns(TO_ID, NAME)
                .whereColumn(FROM_ID).isEqualTo(literal(nodeUuid))
                .build());
        for (Row row : resultSet) {
            dependencies.put(row.getString(1), row.getUuid(0));
        }
        return dependencies;
    }

    @Override
    public Set<NodeInfo> getDependencies(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        Set<NodeInfo> dependencies = new HashSet<>();
        ResultSet resultSet = getSession().execute(selectFrom(DEPENDENCIES)
                .column(TO_ID)
                .whereColumn(FROM_ID).isEqualTo(literal(nodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .build());
        for (Row row : resultSet) {
            UUID toNodeUuid = row.getUuid(0);
            try {
                dependencies.add(getNodeInfo(toNodeUuid));
            } catch (CassandraAfsException e) {
                LOGGER.warn(BROKEN_DEPENDENCY, e);
            }
        }
        return dependencies;
    }

    @Override
    public Set<NodeDependency> getDependencies(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Set<NodeDependency> dependencies = new HashSet<>();
        ResultSet resultSet = getSession().execute(selectFrom(DEPENDENCIES)
                .columns(TO_ID, NAME)
                .whereColumn(FROM_ID).isEqualTo(literal(nodeUuid))
                .build());
        for (Row row : resultSet) {
            UUID toNodeUuid = row.getUuid(0);
            String name = row.getString(1);
            try {
                dependencies.add(new NodeDependency(name, getNodeInfo(toNodeUuid)));
            } catch (CassandraAfsException e) {
                LOGGER.warn(BROKEN_DEPENDENCY, e);
            }
        }
        return dependencies;
    }

    @Override
    public Set<NodeInfo> getBackwardDependencies(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Set<NodeInfo> backwardDependencies = new HashSet<>();
        ResultSet resultSet = getSession().execute(selectFrom(BACKWARD_DEPENDENCIES)
                .column(FROM_ID)
                .whereColumn(TO_ID).isEqualTo(literal(nodeUuid))
                .build());
        for (Row row : resultSet) {
            try {
                backwardDependencies.add(getNodeInfo(row.getUuid(0)));
            } catch (CassandraAfsException e) {
                LOGGER.warn(BROKEN_DEPENDENCY, e);
            }
        }
        return backwardDependencies;
    }

    @Override
    public void removeDependency(String nodeId, String name, String toNodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        UUID toNodeUuid = checkNodeId(toNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);

        batchStatementBuilder.addStatement(deleteFrom(DEPENDENCIES)
                .whereColumn(FROM_ID).isEqualTo(literal(nodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .whereColumn(TO_ID).isEqualTo(literal(toNodeUuid))
                .build());
        batchStatementBuilder.addStatement(deleteFrom(BACKWARD_DEPENDENCIES)
                .whereColumn(TO_ID).isEqualTo(literal(toNodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .whereColumn(FROM_ID).isEqualTo(literal(nodeUuid))
                .build());

        getSession().execute(batchStatementBuilder.build());
        pushEvent(new DependencyRemoved(nodeId, name), APPSTORAGE_DEPENDENCY_TOPIC);
        pushEvent(new BackwardDependencyRemoved(toNodeId, name), APPSTORAGE_DEPENDENCY_TOPIC);
    }

    @Override
    public void flush() {
        changeBuffer.flush();
        eventsBus.flush();
    }

    @Override
    public boolean isClosed() {
        return getSession().isClosed();
    }

    @Override
    public void close() {
        changeBuffer.flush();
        contextSupplier.get().close();
    }

    @Override
    public List<String> getSupportedFileSystemChecks() {
        return ImmutableList.of(FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES,
                REF_NOT_FOUND, ORPHAN_NODE, ORPHAN_DATA);
    }

    @Override
    public List<FileSystemCheckIssue> checkFileSystem(FileSystemCheckOptions options) {
        List<FileSystemCheckIssue> results = new ArrayList<>();

        for (String type : options.getTypes()) {
            switch (type) {
                case FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES:
                    options.getInconsistentNodesExpirationTime()
                            .ifPresent(time -> checkInconsistent(results, time, options.isRepair()));
                    break;
                case REF_NOT_FOUND:
                    checkReferenceNotFound(results, options);
                    break;
                case ORPHAN_NODE:
                    checkOrphanNode(results, options);
                    break;
                case ORPHAN_DATA:
                    checkOrphanData(results, options);
                    break;
                default:
                    LOGGER.warn("Check {} not supported in {}", type, getClass());
            }
        }

        return results;
    }

    private void checkOrphanData(List<FileSystemCheckIssue> results, FileSystemCheckOptions options) {
        Set<UUID> existingNodeIds = new HashSet<>();
        Set<UUID> orphanDataIds = new HashSet<>();
        ResultSet existingNodes = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .distinct()
                .column(ID)
                .build());
        for (Row row : existingNodes) {
            existingNodeIds.add(row.getUuid(ID));
        }
        ResultSet nodeDatas = getSession().execute(selectFrom(NODE_DATA)
                .distinct()
                .columns(ID, NAME)
                .build());
        for (Row row : nodeDatas) {
            UUID uuid = row.getUuid(ID);
            if (!existingNodeIds.contains(uuid)) {
                orphanDataIds.add(uuid);
                FileSystemCheckIssue issue = new FileSystemCheckIssue().setNodeName("N/A")
                        .setNodeId(uuid.toString())
                        .setType(ORPHAN_DATA)
                        .setDescription("Orphan data(" + row.getString(NAME) + ") is binding to non-existing node(" + uuid + ")")
                        .setRepaired(options.isRepair());
                if (options.isRepair()) {
                    issue.setRepaired(true)
                            .setResolutionDescription("Delete orphan data(" + row.getString(NAME) + ").");
                }
                results.add(issue);
            }
        }
        if (options.isRepair()) {
            BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
            for (UUID id : orphanDataIds) {
                removeAllData(id, batchStatementBuilder);
            }
            getSession().execute(batchStatementBuilder.build());
        }
    }

    private void checkOrphanNode(List<FileSystemCheckIssue> results, FileSystemCheckOptions options) {
        // get all child id which parent name is null
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .columns(ID, CHILD_ID, NAME, CHILD_NAME)
                .build());
        List<UUID> orphanIds = new ArrayList<>();
        Set<UUID> fakeParentIds = new HashSet<>();
        for (Row row : resultSet) {
            if (row.getString(NAME) == null) {
                UUID nodeId = row.getUuid(CHILD_ID);
                String nodeName = row.getString(CHILD_NAME);
                UUID fakeParentId = row.getUuid(ID);
                FileSystemCheckIssue issue = new FileSystemCheckIssue().setNodeId(nodeId.toString())
                        .setNodeName(nodeName)
                        .setType(ORPHAN_NODE)
                        .setDescription(nodeName + "(" + nodeId + ") is an orphan node. Its fake parent id=" + fakeParentId);
                if (options.isRepair()) {
                    orphanIds.add(nodeId);
                    fakeParentIds.add(fakeParentId);
                    issue.setRepaired(true);
                    issue.setResolutionDescription("Deleted node [name=" + nodeName + ", id=" + nodeId + "] and reference to null name node [id=" + fakeParentId + "]");
                }
                results.add(issue);
            }
        }
        if (options.isRepair()) {
            orphanIds.forEach(this::deleteNode);
            for (UUID fakeParentId : fakeParentIds) {
                getSession().execute(deleteFrom(CHILDREN_BY_NAME_AND_CLASS)
                        .whereColumn(ID).isEqualTo(literal(fakeParentId))
                        .build());
            }
        }
    }

    private void checkReferenceNotFound(List<FileSystemCheckIssue> results, FileSystemCheckOptions options) {
        List<SimpleStatement> statements = new ArrayList<>();
        Set<ChildNodeInfo> notFoundIds = new HashSet<>();
        Set<UUID> existingRows = allPrimaryKeys();
        for (ChildNodeInfo entity : getAllIdsInChildId()) {
            if (!existingRows.contains(entity.id)) {
                notFoundIds.add(entity);
            }
        }

        for (ChildNodeInfo childNodeInfo : notFoundIds) {
            final UUID childId = childNodeInfo.id;
            final FileSystemCheckIssue issue = new FileSystemCheckIssue()
                    .setNodeId(childId.toString())
                    .setNodeName(childNodeInfo.name)
                    .setRepaired(options.isRepair())
                    .setDescription("row is not found but still referenced in " + childNodeInfo.parentId)
                    .setType(REF_NOT_FOUND);
            results.add(issue);
            if (options.isRepair()) {
                statements.add(deleteFrom(CHILDREN_BY_NAME_AND_CLASS)
                        .whereColumn(ID).isEqualTo(literal(childNodeInfo.parentId))
                        .whereColumn(CHILD_NAME).isEqualTo(literal(childNodeInfo.name))
                        .build());
                issue.setResolutionDescription("reset null child_name and child_id in " + childNodeInfo.parentId);
            }
        }
        if (options.isRepair()) {
            executeStatements(statements);
        }
    }

    private void executeStatements(List<SimpleStatement> statements) {
        for (Statement statement : statements) {
            getSession().execute(statement);
        }
    }

    private Set<ChildNodeInfo> getAllIdsInChildId() {
        Set<ChildNodeInfo> set = new HashSet<>();
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .columns(CHILD_ID, CHILD_NAME, ID)
                .build());
        for (Row row : resultSet) {
            final UUID id = row.getUuid(CHILD_ID);
            if (id != null) {
                set.add(new ChildNodeInfo(id, row.getString(CHILD_NAME), row.getUuid(ID)));
            }
        }
        return set;
    }

    private Set<UUID> allPrimaryKeys() {
        Set<UUID> set = new HashSet<>();
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .distinct()
                .column(ID)
                .build());
        for (Row row : resultSet) {
            set.add(row.getUuid(0));
        }
        return set;
    }

    private void checkInconsistent(List<FileSystemCheckIssue> results, Instant expirationTime, boolean repair) {
        ResultSet resultSet = getSession().execute(selectFrom(CHILDREN_BY_NAME_AND_CLASS)
                .distinct()
                .columns(ID, NAME, MODIFICATION_DATE, CONSISTENT)
                .build());
        for (Row row : resultSet) {
            final Optional<FileSystemCheckIssue> issue = buildExpirationInconsistentIssue(row, expirationTime);
            issue.ifPresent(results::add);
        }
        if (repair) {
            for (FileSystemCheckIssue issue : results) {
                if (Objects.equals(issue.getType(), "inconsistent")) {
                    repairExpirationInconsistent(issue);
                }
            }
        }
    }

    private static Optional<FileSystemCheckIssue> buildExpirationInconsistentIssue(Row row, Instant instant) {
        return Optional.ofNullable(buildExpirationInconsistent(row, instant));
    }

    private static FileSystemCheckIssue buildExpirationInconsistent(Row row, Instant instant) {
        if (row.getInstant(MODIFICATION_DATE).isBefore(instant) && !row.getBool(CONSISTENT)) {
            final FileSystemCheckIssue fileSystemCheckIssue = buildIssue(row);
            fileSystemCheckIssue.setType("inconsistent");
            fileSystemCheckIssue.setDescription("inconsistent and older than " + instant);
            return fileSystemCheckIssue;
        }
        return null;
    }

    private static FileSystemCheckIssue buildIssue(Row row) {
        final FileSystemCheckIssue issue = new FileSystemCheckIssue();
        issue.setNodeId(row.getUuid(ID).toString()).setNodeName(row.getString(NAME));
        return issue;
    }

    private void repairExpirationInconsistent(FileSystemCheckIssue issue) {
        deleteNode(issue.getNodeId());
        issue.setRepaired(true);
        issue.setResolutionDescription("deleted");
    }

    static class ChildNodeInfo {

        final UUID id;
        final String name;
        final UUID parentId;

        ChildNodeInfo(UUID id, String name, UUID parentId) {
            this.id = id;
            this.name = name;
            this.parentId = parentId;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ChildNodeInfo)) {
                return false;
            }

            ChildNodeInfo that = (ChildNodeInfo) o;

            return id.equals(that.id);
        }
    }
}
