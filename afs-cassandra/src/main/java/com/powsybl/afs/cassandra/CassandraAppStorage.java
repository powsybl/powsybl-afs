/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.buffer.*;
import com.powsybl.afs.storage.events.*;
import com.powsybl.timeseries.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.powsybl.afs.cassandra.CassandraConstants.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraAppStorage extends AbstractAppStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAppStorage.class);

    private static final String BROKEN_DEPENDENCY = "Broken dependency";

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
            createTimeSeriesPreparedStmt = getSession().prepare(insertInto(REGULAR_TIME_SERIES)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(DATA_TYPE, bindMarker())
                    .value(TIME_SERIES_TAGS, bindMarker())
                    .value(START, bindMarker())
                    .value(END, bindMarker())
                    .value(SPACING, bindMarker()));

            insertTimeSeriesDataChunksPreparedStmt = getSession().prepare(insertInto(TIME_SERIES_DATA_CHUNK_TYPES)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(VERSION, bindMarker())
                    .value(CHUNK_ID, bindMarker())
                    .value(CHUNK_TYPE, bindMarker()));

            insertDoubleTimeSeriesDataCompressedChunksPreparedStmt = getSession().prepare(insertInto(DOUBLE_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(VERSION, bindMarker())
                    .value(CHUNK_ID, bindMarker())
                    .value(OFFSET, bindMarker())
                    .value(UNCOMPRESSED_LENGTH, bindMarker())
                    .value(STEP_VALUES, bindMarker())
                    .value(STEP_LENGTHS, bindMarker()));

            insertDoubleTimeSeriesDataUncompressedChunksPreparedStmt = getSession().prepare(insertInto(DOUBLE_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(VERSION, bindMarker())
                    .value(CHUNK_ID, bindMarker())
                    .value(OFFSET, bindMarker())
                    .value(VALUES, bindMarker()));

            insertStringTimeSeriesDataCompressedChunksPreparedStmt = getSession().prepare(insertInto(STRING_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(VERSION, bindMarker())
                    .value(CHUNK_ID, bindMarker())
                    .value(OFFSET, bindMarker())
                    .value(UNCOMPRESSED_LENGTH, bindMarker())
                    .value(STEP_VALUES, bindMarker())
                    .value(STEP_LENGTHS, bindMarker()));

            insertStringTimeSeriesDataUncompressedChunksPreparedStmt = getSession().prepare(insertInto(STRING_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                    .value(ID, bindMarker())
                    .value(TIME_SERIES_NAME, bindMarker())
                    .value(VERSION, bindMarker())
                    .value(CHUNK_ID, bindMarker())
                    .value(OFFSET, bindMarker())
                    .value(VALUES, bindMarker()));
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
                statements.add(preparedStatementsSupplier.get().createTimeSeriesPreparedStmt.bind()
                        .setUUID(0, checkNodeId(creation.getNodeId()))
                        .setString(1, creation.getMetadata().getName())
                        .setString(2, creation.getMetadata().getDataType().name())
                        .setMap(3, creation.getMetadata().getTags())
                        .setTimestamp(4, new Date(index.getStartTime()))
                        .setTimestamp(5, new Date(index.getEndTime()))
                        .setLong(6, index.getSpacing()));
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
                UUID chunkId = UUIDs.timeBased();

                // skip empty chunks
                if (chunk.getLength() == 0) {
                    LOGGER.warn("Empty chunk for time series {} version {} of node {}", timeSeriesName, version, nodeUuid);
                    continue;
                }

                statements.add(preparedStatementsSupplier.get().insertTimeSeriesDataChunksPreparedStmt.bind()
                        .setUUID(0, nodeUuid)
                        .setString(1, timeSeriesName)
                        .setInt(2, version)
                        .setUUID(3, chunkId)
                        .setInt(4, chunk.isCompressed() ? TimeSeriesChunkType.DOUBLE_COMPRESSED.ordinal()
                                                        : TimeSeriesChunkType.DOUBLE_UNCOMPRESSED.ordinal()));

                if (chunk.isCompressed()) {
                    statements.add(preparedStatementsSupplier.get().insertDoubleTimeSeriesDataCompressedChunksPreparedStmt.bind()
                            .setUUID(0, nodeUuid)
                            .setString(1, timeSeriesName)
                            .setInt(2, version)
                            .setUUID(3, chunkId)
                            .setInt(4, chunk.getOffset())
                            .setInt(5, chunk.getLength())
                            .setList(6, Arrays.stream(((CompressedDoubleDataChunk) chunk).getStepValues()).boxed().collect(Collectors.toList()))
                            .setList(7, Arrays.stream(((CompressedDoubleDataChunk) chunk).getStepLengths()).boxed().collect(Collectors.toList())));
                } else {
                    statements.add(preparedStatementsSupplier.get().insertDoubleTimeSeriesDataUncompressedChunksPreparedStmt.bind()
                            .setUUID(0, nodeUuid)
                            .setString(1, timeSeriesName)
                            .setInt(2, version)
                            .setUUID(3, chunkId)
                            .setInt(4, chunk.getOffset())
                            .setList(5, Arrays.stream(((UncompressedDoubleDataChunk) chunk).getValues()).boxed().collect(Collectors.toList())));
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
                UUID chunkId = UUIDs.timeBased();

                // skip empty chunks
                if (chunk.getLength() == 0) {
                    LOGGER.warn("Empty chunk for time series {} version {} of node {}", timeSeriesName, version, nodeUuid);
                    continue;
                }

                statements.add(preparedStatementsSupplier.get().insertTimeSeriesDataChunksPreparedStmt.bind()
                        .setUUID(0, nodeUuid)
                        .setString(1, timeSeriesName)
                        .setInt(2, version)
                        .setUUID(3, chunkId)
                        .setInt(4, chunk.isCompressed() ? TimeSeriesChunkType.STRING_COMPRESSED.ordinal()
                                                        : TimeSeriesChunkType.STRING_UNCOMPRESSED.ordinal()));

                if (chunk.isCompressed()) {
                    statements.add(preparedStatementsSupplier.get().insertStringTimeSeriesDataCompressedChunksPreparedStmt.bind()
                            .setUUID(0, nodeUuid)
                            .setString(1, timeSeriesName)
                            .setInt(2, version)
                            .setUUID(3, chunkId)
                            .setInt(4, chunk.getOffset())
                            .setInt(5, chunk.getLength())
                            .setList(6, fixNullValues(((CompressedStringDataChunk) chunk).getStepValues()))
                            .setList(7, Arrays.stream(((CompressedStringDataChunk) chunk).getStepLengths()).boxed().collect(Collectors.toList())));
                } else {
                    statements.add(preparedStatementsSupplier.get().insertStringTimeSeriesDataUncompressedChunksPreparedStmt.bind()
                            .setUUID(0, nodeUuid)
                            .setString(1, timeSeriesName)
                            .setInt(2, version)
                            .setUUID(3, chunkId)
                            .setInt(4, chunk.getOffset())
                            .setList(5, fixNullValues(((UncompressedStringDataChunk) chunk).getValues())));
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

    private Session getSession() {
        return Objects.requireNonNull(contextSupplier.get()).getSession();
    }

    @Override
    public NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(nodePseudoClass);
        // check if root node with same name has already been created
        ResultSet resultSet = getSession().execute(insertInto(ROOT_NODE)
                .ifNotExists()
                .value(ROOT_ID, now())
                .value(FS_NAME, fileSystemName));
        boolean rootCreated = resultSet.wasApplied();

        resultSet = getSession().execute(select(ROOT_ID)
                .from(ROOT_NODE)
                .where(eq(FS_NAME, fileSystemName)));
        UUID rootNodeUuid = resultSet.one().getUUID(0);

        NodeInfo rootNodeInfo;
        if (rootCreated) {
            BatchStatement batchStatement = new BatchStatement();
            rootNodeInfo = createNode(rootNodeUuid, null, name, nodePseudoClass, "", 0, new NodeGenericMetadata(), batchStatement);
            getSession().execute(batchStatement);
            setConsistent(rootNodeInfo.getId());
        } else {
            rootNodeInfo = getNodeInfo(rootNodeUuid);
        }

        return rootNodeInfo;
    }

    private String getNodeName(UUID nodeUuid) {
        Objects.requireNonNull(nodeUuid);
        ResultSet resultSet = getSession().execute(select(NAME).from(CHILDREN_BY_NAME_AND_CLASS)
                                                                         .where(eq(ID, nodeUuid)));
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
        return row.isNull(i) || row.getBool(i);
    }

    @Override
    public boolean isConsistent(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(nodeUuid);
        ResultSet resultSet = getSession().execute(select(CONSISTENT)
                .from(CHILDREN_BY_NAME_AND_CLASS)
                .where(eq(ID, nodeUuid)));
        Row row = resultSet.one();
        if (row == null) {
            throw createNodeNotFoundException(nodeUuid);
        }
        return isConsistentBackwardCompatible(row, 0);
    }

    private NodeInfo createNode(UUID parentNodeUuid, String name, String nodePseudoClass, String description,
                                int version, NodeGenericMetadata genericMetadata, BatchStatement batchStatement) {
        return createNode(UUIDs.timeBased(), parentNodeUuid, name, nodePseudoClass, description, version, genericMetadata, batchStatement);
    }

    private NodeInfo createNode(UUID nodeUuid, UUID parentNodeUuid, String name, String nodePseudoClass,
                                String description, int version, NodeGenericMetadata genericMetadata,
                                BatchStatement batchStatement) {
        long creationTime = ZonedDateTime.now().toInstant().toEpochMilli();
        batchStatement.add(insertInto(CHILDREN_BY_NAME_AND_CLASS).value(ID, nodeUuid)
                                                                 .value(NAME, name)
                                                                 .value(PARENT_ID, parentNodeUuid)
                                                                 .value(PSEUDO_CLASS, nodePseudoClass)
                                                                 .value(DESCRIPTION, description)
                                                                 .value(CONSISTENT, false)
                                                                 .value(CREATION_DATE, new Date(creationTime))
                                                                 .value(MODIFICATION_DATE, new Date(creationTime))
                                                                 .value(VERSION, version)
                                                                 .value(MT, genericMetadata.getStrings())
                                                                 .value(MD, genericMetadata.getDoubles())
                                                                 .value(MI, genericMetadata.getInts())
                                                                 .value(MB, genericMetadata.getBooleans()));
        if (parentNodeUuid != null) {
            batchStatement.add(insertInto(CHILDREN_BY_NAME_AND_CLASS).value(ID, parentNodeUuid)
                                                                     .value(CHILD_NAME, name)
                                                                     .value(CHILD_ID, nodeUuid)
                                                                     .value(CHILD_PSEUDO_CLASS, nodePseudoClass)
                                                                     .value(CHILD_DESCRIPTION, description)
                                                                     .value(CHILD_CONSISTENT, false)
                                                                     .value(CHILD_CREATION_DATE, new Date(creationTime))
                                                                     .value(CHILD_MODIFICATION_DATE, new Date(creationTime))
                                                                     .value(CHILD_VERSION, version)
                                                                     .value(CMT, genericMetadata.getStrings())
                                                                     .value(CMD, genericMetadata.getDoubles())
                                                                     .value(CMI, genericMetadata.getInts())
                                                                     .value(CMB, genericMetadata.getBooleans()));
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

        BatchStatement batchStatement = new BatchStatement();
        NodeInfo nodeInfo = createNode(parentNodeUuid, name, nodePseudoClass, description, version, genericMetadata, batchStatement);
        getSession().execute(batchStatement);
        return nodeInfo;
    }

    @Override
    public void setMetadata(String nodeId, NodeGenericMetadata genericMetadata) {
        UUID nodeUuid = checkNodeId(nodeId);
        NodeGenericMetadata newMetadata = genericMetadata != null ? genericMetadata : new NodeGenericMetadata();

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(MD, newMetadata.getDoubles()))
                .where(eq(ID, nodeUuid)));
        batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(MT, newMetadata.getStrings()))
                .where(eq(ID, nodeUuid)));
        batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(MI, newMetadata.getInts()))
                .where(eq(ID, nodeUuid)));
        batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(MB, newMetadata.getBooleans()))
                .where(eq(ID, nodeUuid)));

        UUID parentNodeUuid = getParentNodeUuid(nodeUuid);
        if (parentNodeUuid != null) {
            NodeInfo nodeInfo = getNodeInfo(nodeId);
            batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(CMD, newMetadata.getDoubles()))
                    .where(eq(ID, parentNodeUuid))
                    .and(eq(CHILD_NAME, nodeInfo.getName()))
                    .and(eq(CHILD_PSEUDO_CLASS, nodeInfo.getPseudoClass())));
            batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(CMT, newMetadata.getStrings()))
                    .where(eq(ID, parentNodeUuid))
                    .and(eq(CHILD_NAME, nodeInfo.getName()))
                    .and(eq(CHILD_PSEUDO_CLASS, nodeInfo.getPseudoClass())));
            batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(CMI, newMetadata.getInts()))
                    .where(eq(ID, parentNodeUuid))
                    .and(eq(CHILD_NAME, nodeInfo.getName()))
                    .and(eq(CHILD_PSEUDO_CLASS, nodeInfo.getPseudoClass())));
            batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(CMB, newMetadata.getBooleans()))
                    .where(eq(ID, parentNodeUuid))
                    .and(eq(CHILD_NAME, nodeInfo.getName()))
                    .and(eq(CHILD_PSEUDO_CLASS, nodeInfo.getPseudoClass())));
        }

        getSession().execute(batchStatement);
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
            getSession().execute(delete().from(CHILDREN_BY_NAME_AND_CLASS)
                    .where(eq(ID, parentNodeUuid))
                    .and(eq(CHILD_NAME, nodeInfo.getName())));
            getSession().execute(insertInto(CHILDREN_BY_NAME_AND_CLASS).value(ID, parentNodeUuid)
                    .value(CHILD_NAME, name)
                    .value(CHILD_ID, nodeUuid)
                    .value(CHILD_PSEUDO_CLASS, nodeInfo.getPseudoClass())
                    .value(CHILD_DESCRIPTION, nodeInfo.getDescription())
                    .value(CHILD_CREATION_DATE, new Date(nodeInfo.getCreationTime()))
                    .value(CHILD_MODIFICATION_DATE, new Date(nodeInfo.getModificationTime()))
                    .value(CHILD_VERSION, nodeInfo.getVersion())
                    .value(CMT, nodeInfo.getGenericMetadata().getStrings())
                    .value(CMD, nodeInfo.getGenericMetadata().getDoubles())
                    .value(CMI, nodeInfo.getGenericMetadata().getInts())
                    .value(CMB, nodeInfo.getGenericMetadata().getBooleans()));
            getSession().execute(update(CHILDREN_BY_NAME_AND_CLASS).with(set(NAME, name))
                    .where(eq(ID, nodeUuid)));
        });
        pushEvent(new NodeNameUpdated(nodeId, name), APPSTORAGE_NODE_TOPIC);
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
        ResultSet resultSet = getSession().execute(select(NAME, PSEUDO_CLASS, DESCRIPTION, CREATION_DATE,
                                                                  MODIFICATION_DATE, VERSION, MT, MD, MI, MB)
                .from(CHILDREN_BY_NAME_AND_CLASS)
                .where(eq(ID, nodeUuid)));
        Row row = resultSet.one();
        if (row == null) {
            throw createNodeNotFoundException(nodeUuid);
        }
        return new NodeInfo(nodeUuid.toString(),
                            row.getString(0),
                            row.getString(1),
                            row.getString(2),
                            row.getTimestamp(3).getTime(),
                            row.getTimestamp(4).getTime(),
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
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(attributeName, newValue))
                .where(eq(ID, nodeUuid)));
        if (parentNodeId != null) {
            NodeInfo nodeInfo = getNodeInfo(nodeId);
            batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(childAttributeName, newValue))
                    .where(eq(ID, parentNodeId))
                    .and(eq(CHILD_NAME, nodeInfo.getName()))
                    .and(eq(CHILD_PSEUDO_CLASS, nodeInfo.getPseudoClass())));
        }
        getSession().execute(batchStatement);
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
        ResultSet resultSet = getSession().execute(select(CHILD_ID, CHILD_CONSISTENT)
                .from(CHILDREN_BY_NAME_AND_CLASS)
                .where(eq(ID, nodeUuid)));
        for (Row row : resultSet) {
            UUID uuid = row.getUUID(0);
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
        ResultSet resultSet = getSession().execute(select(CHILD_NAME, CHILD_PSEUDO_CLASS, CHILD_ID, CHILD_DESCRIPTION,
                CHILD_CREATION_DATE, CHILD_MODIFICATION_DATE, CHILD_VERSION,
                CMT, CMD, CMI, CMB, CHILD_CONSISTENT)
                .from(CHILDREN_BY_NAME_AND_CLASS)
                .where(eq(ID, nodeUuid)));
        for (Row row : resultSet) {
            UUID uuid = row.getUUID(2);
            if (uuid != null && isConsistentBackwardCompatible(row, 11)) {
                NodeInfo nodeInfo = new NodeInfo(uuid.toString(),
                        row.getString(0),
                        row.getString(1),
                        row.getString(3),
                        row.getTimestamp(4).getTime(),
                        row.getTimestamp(5).getTime(),
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
        ResultSet resultSet = getSession().execute(select(CHILD_NAME, CHILD_PSEUDO_CLASS, CHILD_ID, CHILD_DESCRIPTION,
                CHILD_CREATION_DATE, CHILD_MODIFICATION_DATE, CHILD_VERSION,
                CMT, CMD, CMI, CMB, CHILD_CONSISTENT)
                .from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : resultSet) {
            UUID uuid = row.getUUID(2);
            if (uuid != null && !isConsistentBackwardCompatible(row, 11)) {
                NodeInfo nodeInfo = new NodeInfo(uuid.toString(),
                        row.getString(0),
                        row.getString(1),
                        row.getString(3),
                        row.getTimestamp(4).getTime(),
                        row.getTimestamp(5).getTime(),
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
        ResultSet resultSet = getSession().execute(select(CHILD_ID, CHILD_PSEUDO_CLASS, CHILD_DESCRIPTION,
                                                                  CHILD_CREATION_DATE, CHILD_MODIFICATION_DATE, CHILD_VERSION,
                                                                  CMT, CMD, CMI, CMB, CHILD_CONSISTENT)
                .from(CHILDREN_BY_NAME_AND_CLASS)
                .where(eq(ID, nodeUuid))
                .and(eq(CHILD_NAME, name)));
        Row row = resultSet.one();
        if (row == null || !isConsistentBackwardCompatible(row, 10)) {
            return Optional.empty();
        }
        return Optional.of(new NodeInfo(row.getUUID(0).toString(),
                                        name,
                                        row.getString(1),
                                        row.getString(2),
                                        row.getTimestamp(3).getTime(),
                                        row.getTimestamp(4).getTime(),
                                        row.getInt(5),
                                        new NodeGenericMetadata(row.getMap(6, String.class, String.class),
                                                row.getMap(7, String.class, Double.class),
                                                row.getMap(8, String.class, Integer.class),
                                                row.getMap(9, String.class, Boolean.class))));
    }

    private UUID getParentNodeUuid(UUID nodeUuid) {
        ResultSet resultSet = getSession().execute(select(PARENT_ID, CHILD_CONSISTENT).from(CHILDREN_BY_NAME_AND_CLASS)
                .where(eq(ID, nodeUuid)));
        Row row = resultSet.one();
        if (row == null) {
            throw createNodeNotFoundException(nodeUuid);
        }
        if (isConsistentBackwardCompatible(row, 1)) {
            return row.getUUID(0);
        }
        return null;
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
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(update(CHILDREN_BY_NAME_AND_CLASS).with(set(PARENT_ID, newParentNodeUuid))
                                                               .where(eq(ID, nodeUuid)));
        batchStatement.add(delete().from(CHILDREN_BY_NAME_AND_CLASS).where(eq(ID, getParentNodeUuid(nodeUuid)))
                                                                   .and(eq(CHILD_NAME, nodeInfo.getName())));
        batchStatement.add(insertInto(CHILDREN_BY_NAME_AND_CLASS).value(ID, newParentNodeUuid)
                                                                   .value(CHILD_NAME, nodeInfo.getName())
                                                                   .value(CHILD_PSEUDO_CLASS, nodeInfo.getPseudoClass())
                                                                   .value(CHILD_ID, nodeUuid)
                                                                   .value(CHILD_DESCRIPTION, nodeInfo.getDescription())
                                                                   .value(CHILD_CREATION_DATE, new Date(nodeInfo.getCreationTime()))
                                                                   .value(CHILD_MODIFICATION_DATE, new Date(nodeInfo.getModificationTime()))
                                                                   .value(CHILD_VERSION, nodeInfo.getVersion())
                                                                   .value(CMT, nodeInfo.getGenericMetadata().getStrings())
                                                                   .value(CMD, nodeInfo.getGenericMetadata().getDoubles())
                                                                   .value(CMI, nodeInfo.getGenericMetadata().getInts())
                                                                   .value(CMB, nodeInfo.getGenericMetadata().getBooleans()));
        getSession().execute(batchStatement);

        pushEvent(new ParentChanged(nodeId), APPSTORAGE_NODE_TOPIC);
    }

    @Override
    public String deleteNode(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        UUID parentNodeUuid = deleteNode(nodeUuid);
        pushEvent(new NodeRemoved(nodeId, String.valueOf(parentNodeUuid)), APPSTORAGE_NODE_TOPIC);
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

        List<Statement> statements = new ArrayList<>();

        // children
        statements.add(delete().from(CHILDREN_BY_NAME_AND_CLASS)
                               .where(eq(ID, nodeUuid)));
        UUID parentNodeUuid = getParentNodeUuid(nodeUuid);
        statements.add(delete().from(CHILDREN_BY_NAME_AND_CLASS)
                               .where(eq(ID, parentNodeUuid))
                               .and(eq(CHILD_NAME, getNodeName(nodeUuid))));

        // data
        removeAllData(nodeUuid, statements);

        // time series
        clearTimeSeries(nodeUuid, statements);

        // dependencies
        ResultSet resultSet = getSession().execute(select(NAME, FROM_ID).from(BACKWARD_DEPENDENCIES)
                                                                                .where(eq(TO_ID, nodeUuid)));
        for (Row row : resultSet) {
            String name = row.getString(0);
            UUID otherNodeUuid = row.getUUID(1);
            statements.add(delete().from(DEPENDENCIES).where(eq(FROM_ID, otherNodeUuid))
                                                      .and(eq(NAME, name))
                                                      .and(eq(TO_ID, nodeUuid)));
        }
        statements.add(delete().from(DEPENDENCIES).where(eq(FROM_ID, nodeUuid)));
        statements.add(delete().from(BACKWARD_DEPENDENCIES).where(in(TO_ID, getDependencyNodeUuids(nodeUuid))));

        for (Statement statement : statements) {
            getSession().execute(statement);
        }

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
            buffer = new ByteArrayInputStream(firstRow.getBytes(0).array());
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
                ResultSet resultSet = getSession().execute(select(CHUNK).from(NODE_DATA)
                        .where(eq(ID, nodeUuid))
                        .and(eq(NAME, name))
                        .and(eq(CHUNK_NUM, chunkNum)));
                Row row = resultSet.one();
                if (row != null) {
                    try {
                        gzis.close();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    byte[] array = row.getBytes(0).array();
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
        ResultSet resultSet = getSession().execute(select(CHUNK).from(NODE_DATA)
                .where(eq(ID, nodeUuid))
                .and(eq(NAME, name))
                .and(eq(CHUNK_NUM, 0)));
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
            getSession().execute(insertInto(NODE_DATA)
                    .value(ID, nodeUuid)
                    .value(NAME, name)
                    .value(CHUNK_NUM, chunkNum++)
                    .value(CHUNKS_COUNT, chunkNum)
                    .value(CHUNK, ByteBuffer.wrap(buffer.toByteArray())));
            buffer = new ByteArrayOutputStream(config.getBinaryDataChunkSize());
            try {
                gzos = new GZIPOutputStream(buffer);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void executeIfNecessary() {
            if (count > config.getBinaryDataChunkSize()) {
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
            gzos.write(b, off, len);
            count += len;
            executeIfNecessary();
        }

        @Override
        public void close() {
            if (chunkNum == 0 || count > 0) { // create  at least on chunk even empty
                execute();
            }

            // update data names
            getSession().execute(insertInto(NODE_DATA_NAMES)
                    .ifNotExists()
                    .value(ID, nodeUuid)
                    .value(NAME, name));
        }
    }

    @Override
    public OutputStream writeBinaryData(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        // flush buffer to keep change order
        changeBuffer.flush();
        pushEvent(new NodeDataUpdated(nodeId, name), APPSTORAGE_NODE_TOPIC);
        return new BinaryDataOutputStream(nodeUuid, name);
    }

    @Override
    public boolean dataExists(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        ResultSet resultSet = getSession().execute(select().countAll()
                                                                   .from(NODE_DATA_NAMES)
                                                                   .where(eq(ID, nodeUuid))
                                                                   .and(eq(NAME, name)));
        Row row = resultSet.one();
        return row.getLong(0) > 0;
    }

    private Set<String> getDataNames(UUID nodeUuid) {
        ResultSet resultSet = getSession().execute(select(NAME)
                .from(NODE_DATA_NAMES)
                .where(eq(ID, nodeUuid)));
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

    private void removeData(UUID nodeUuid, String name, List<Statement> statements) {
        statements.add(delete()
                .from(NODE_DATA)
                .where(eq(ID, nodeUuid))
                .and(eq(NAME, name)));
        statements.add(delete()
                .from(NODE_DATA_NAMES)
                .where(eq(ID, nodeUuid))
                .and(eq(NAME, name)));
        pushEvent(new NodeDataRemoved(nodeUuid.toString(), name), APPSTORAGE_NODE_TOPIC);
    }

    private void removeAllData(UUID nodeUuid, List<Statement> statements) {
        for (String dataName : getDataNames(nodeUuid)) {
            removeData(nodeUuid, dataName, statements);
        }
    }

    @Override
    public boolean removeData(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);

        // get chunk num list
        List<Integer> chunks = new ArrayList<>(1);
        ResultSet resultSet = getSession().execute(select(CHUNKS_COUNT)
                .from(NODE_DATA)
                .where(eq(ID, nodeUuid))
                .and(eq(NAME, name)));
        for (Row row : resultSet) {
            chunks.add(row.getInt(0));
        }
        if (chunks.isEmpty()) {
            return false;
        }

        List<Statement> statements = new ArrayList<>();
        removeData(nodeUuid, name, statements);
        for (Statement statement : statements) {
            getSession().execute(statement);
        }

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
        ResultSet resultSet = getSession().execute(select(TIME_SERIES_NAME)
                .from(REGULAR_TIME_SERIES)
                .where(eq(ID, nodeUuid)));
        for (Row row : resultSet) {
            timeSeriesNames.add(row.getString(0));
        }
        return timeSeriesNames;
    }

    @Override
    public boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(timeSeriesName);
        ResultSet resultSet = getSession().execute(select().countAll()
                .from(REGULAR_TIME_SERIES)
                .where(eq(ID, nodeUuid)).and(eq(TIME_SERIES_NAME, timeSeriesName)));
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
        ResultSet resultSet = getSession().execute(select(TIME_SERIES_NAME, DATA_TYPE, TIME_SERIES_TAGS, START, END, SPACING)
                .from(REGULAR_TIME_SERIES)
                .where(eq(ID, nodeUuid))
                .and(in(TIME_SERIES_NAME, new ArrayList<>(timeSeriesNames))));
        for (Row row : resultSet) {
            timeSeries.add(new TimeSeriesMetadata(row.getString(0),
                                                  TimeSeriesDataType.valueOf(row.getString(1)),
                                                  row.getMap(2, String.class, String.class),
                                                  new RegularTimeSeriesIndex(row.getTimestamp(3).getTime(),
                                                                             row.getTimestamp(4).getTime(),
                                                                             row.getLong(5))));
        }
        return timeSeries;
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Set<Integer> versions = new TreeSet<>();
        ResultSet resultSet = getSession().execute(select(VERSION)
                .from(TIME_SERIES_DATA_CHUNK_TYPES)
                .where(eq(ID, nodeUuid)));
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
        ResultSet resultSet = getSession().execute(select(VERSION)
                .from(TIME_SERIES_DATA_CHUNK_TYPES)
                .where(eq(ID, nodeUuid))
                .and(eq(TIME_SERIES_NAME, timeSeriesName)));
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
            ResultSet resultSet = getSession().execute(select(TIME_SERIES_NAME, OFFSET, VALUES)
                    .from(DOUBLE_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                    .where(eq(ID, nodeUuid))
                    .and(in(TIME_SERIES_NAME, timeSeriesNamesPartition))
                    .and(eq(VERSION, version)));
            for (Row row : resultSet) {
                String name = row.getString(0);
                int offset = row.getInt(1);
                List<Double> values = row.getList(2, Double.class);
                timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new UncompressedDoubleDataChunk(offset, values.stream().mapToDouble(Double::valueOf).toArray()));
            }

            resultSet = getSession().execute(select(TIME_SERIES_NAME, OFFSET, UNCOMPRESSED_LENGTH, STEP_VALUES, STEP_LENGTHS)
                    .from(DOUBLE_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                    .where(eq(ID, nodeUuid))
                    .and(in(TIME_SERIES_NAME, timeSeriesNamesPartition))
                    .and(eq(VERSION, version)));
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
            ResultSet resultSet = getSession().execute(select(TIME_SERIES_NAME, OFFSET, VALUES)
                .from(STRING_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                .where(eq(ID, nodeUuid))
                .and(in(TIME_SERIES_NAME, timeSeriesNamesPartition))
                .and(eq(VERSION, version)));
            for (Row row : resultSet) {
                String name = row.getString(0);
                int offset = row.getInt(1);
                List<String> values = row.getList(2, String.class);
                timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new UncompressedStringDataChunk(offset, values.toArray(new String[values.size()])));
            }

            resultSet = getSession().execute(select(TIME_SERIES_NAME, OFFSET, UNCOMPRESSED_LENGTH, STEP_VALUES, STEP_LENGTHS)
                    .from(STRING_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                    .where(eq(ID, nodeUuid))
                    .and(in(TIME_SERIES_NAME, timeSeriesNamesPartition))
                    .and(eq(VERSION, version)));
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

    private void clearTimeSeries(UUID nodeUuid, List<Statement> statements) {
        statements.add(delete().from(REGULAR_TIME_SERIES).where(eq(ID, nodeUuid)));
        ResultSet resultSet = getSession().execute(select(TIME_SERIES_NAME, VERSION, CHUNK_TYPE)
                .from(TIME_SERIES_DATA_CHUNK_TYPES)
                .where(eq(ID, nodeUuid)));
        for (Row row : resultSet) {
            String timeSeriesName = row.getString(0);
            int version = row.getInt(1);
            TimeSeriesChunkType chunkType = TimeSeriesChunkType.values()[row.getInt(2)];
            switch (chunkType) {
                case DOUBLE_UNCOMPRESSED:
                    statements.add(delete().from(DOUBLE_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                            .where(eq(ID, nodeUuid)).and(eq(TIME_SERIES_NAME, timeSeriesName))
                                                    .and(eq(VERSION, version)));
                    break;
                case DOUBLE_COMPRESSED:
                    statements.add(delete().from(DOUBLE_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                            .where(eq(ID, nodeUuid)).and(eq(TIME_SERIES_NAME, timeSeriesName))
                                                    .and(eq(VERSION, version)));
                    break;
                case STRING_UNCOMPRESSED:
                    statements.add(delete().from(STRING_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                            .where(eq(ID, nodeUuid)).and(eq(TIME_SERIES_NAME, timeSeriesName))
                                                    .and(eq(VERSION, version)));
                    break;
                case STRING_COMPRESSED:
                    statements.add(delete().from(STRING_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                            .where(eq(ID, nodeUuid)).and(eq(TIME_SERIES_NAME, timeSeriesName))
                                                    .and(eq(VERSION, version)));
                    break;
                default:
                    throw new AssertionError("Unknown chunk type " + chunkType);
            }
        }
        statements.add(delete().from(TIME_SERIES_DATA_CHUNK_TYPES).where(eq(ID, nodeUuid)));
    }

    @Override
    public void clearTimeSeries(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        List<Statement> statements = new ArrayList<>();
        clearTimeSeries(nodeUuid, statements);
        for (Statement statement : statements) {
            getSession().execute(statement);
        }
        pushEvent(new TimeSeriesCleared(nodeUuid.toString()), APPSTORAGE_TIMESERIES_TOPIC);
    }

    @Override
    public void addDependency(String nodeId, String name, String toNodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        UUID toNodeUuid = checkNodeId(toNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatement batchStatement = new BatchStatement();

        batchStatement.add(insertInto(DEPENDENCIES).value(FROM_ID, nodeUuid)
                                                   .value(NAME, name)
                                                   .value(TO_ID, toNodeUuid)
                                                   .value("dep_id", now()));
        batchStatement.add(insertInto(BACKWARD_DEPENDENCIES).value(TO_ID, toNodeUuid)
                                                            .value(NAME, name)
                                                            .value(FROM_ID, nodeUuid));
        getSession().execute(batchStatement);
        pushEvent(new DependencyAdded(nodeId, name),
                String.valueOf(APPSTORAGE_DEPENDENCY_TOPIC));
        pushEvent(new BackwardDependencyAdded(toNodeId, name),
                String.valueOf(APPSTORAGE_DEPENDENCY_TOPIC));
    }

    private List<UUID> getDependencyNodeUuids(UUID nodeUuid) {
        Objects.requireNonNull(nodeUuid);
        List<UUID> dependencies = new ArrayList<>();
        ResultSet resultSet = getSession().execute(select(TO_ID).from(DEPENDENCIES)
                                                                          .where(eq(FROM_ID, nodeUuid)));
        for (Row row : resultSet) {
            dependencies.add(row.getUUID(0));
        }
        return dependencies;
    }

    @Override
    public Set<NodeInfo> getDependencies(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        Set<NodeInfo> dependencies = new HashSet<>();
        ResultSet resultSet = getSession().execute(select(TO_ID)
                .from(DEPENDENCIES)
                .where(eq(FROM_ID, nodeUuid)).and(eq(NAME, name)));
        for (Row row : resultSet) {
            UUID toNodeUuid = row.getUUID(0);
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
        ResultSet resultSet = getSession().execute(select(TO_ID, NAME)
                .from(DEPENDENCIES)
                .where(eq(FROM_ID, nodeUuid)));
        for (Row row : resultSet) {
            UUID toNodeUuid = row.getUUID(0);
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
        ResultSet resultSet = getSession().execute(select(FROM_ID).from(BACKWARD_DEPENDENCIES)
                                                                          .where(eq(TO_ID, nodeUuid)));
        for (Row row : resultSet) {
            try {
                backwardDependencies.add(getNodeInfo(row.getUUID(0)));
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

        BatchStatement batchStatement = new BatchStatement();

        batchStatement.add(delete().from(DEPENDENCIES)
                .where(eq(FROM_ID, nodeUuid))
                .and(eq(NAME, name))
                .and(eq(TO_ID, toNodeUuid)));
        batchStatement.add(delete().from(BACKWARD_DEPENDENCIES)
                .where(eq(TO_ID, toNodeUuid))
                .and(eq(NAME, name))
                .and(eq(FROM_ID, nodeUuid)));

        getSession().execute(batchStatement);
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
}
