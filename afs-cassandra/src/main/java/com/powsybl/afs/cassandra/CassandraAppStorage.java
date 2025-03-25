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
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.buffer.*;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import com.powsybl.afs.storage.events.*;
import com.powsybl.timeseries.*;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.powsybl.afs.cassandra.CassandraConstants.*;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class CassandraAppStorage extends AbstractAppStorage {

    public static final String REF_NOT_FOUND = "REFERENCE_NOT_FOUND";
    public static final String ORPHAN_NODE = "ORPHAN_NODE";
    public static final String ORPHAN_DATA = "ORPHAN_DATA";
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAppStorage.class);
    private static final String BROKEN_DEPENDENCY = "Broken dependency";
    private final String fileSystemName;

    private final Supplier<CassandraContext> contextSupplier;

    private final CassandraAppStorageConfig config;
    private final Supplier<PreparedStatements> preparedStatementsSupplier;
    private final StorageChangeFlusher changeFlusher = new StorageChangeFlusher() {

        private void flush(TimeSeriesCreation creation, List<Statement<?>> statements, TimeSeriesWritingContext writingContext) {
            if (creation.getMetadata().getIndex() instanceof RegularTimeSeriesIndex index) {
                statements.add(preparedStatementsSupplier.get().getCreateTimeSeriesPreparedStmt()
                    .bind()
                    .setUuid(ID, checkNodeId(creation.getNodeId()))
                    .setString(TIME_SERIES_NAME, creation.getMetadata().getName())
                    .setString(DATA_TYPE, creation.getMetadata().getDataType().name())
                    .setMap(TIME_SERIES_TAGS, creation.getMetadata().getTags(), String.class, String.class)
                    .setInstant(START, Instant.ofEpochMilli(index.getStartTime()))
                    .setInstant(END, Instant.ofEpochMilli(index.getEndTime()))
                    .setLong(SPACING, index.getSpacing()));
            } else {
                throw new CassandraAfsException("Flush exception - index is not a regular time series index");
            }

            writingContext.createdTimeSeriesCount++;
        }

        private void flush(DoubleTimeSeriesChunksAddition addition, List<Statement<?>> statements, TimeSeriesWritingContext writingContext) {
            UUID nodeUuid = checkNodeId(addition.getNodeId());
            int version = addition.getVersion();
            String timeSeriesName = addition.getTimeSeriesName();
            List<DoubleDataChunk> chunks = addition.getChunks();

            for (DoubleDataChunk chunk : chunks) {
                UUID chunkId = Uuids.timeBased();

                // skip empty chunks
                if (chunk.getLength() == 0) {
                    logEmptyChunk(timeSeriesName, version, nodeUuid);
                    continue;
                }

                statements.add(preparedStatementsSupplier.get().getInsertTimeSeriesDataChunksPreparedStmt()
                    .bind()
                    .setUuid(ID, nodeUuid)
                    .setString(TIME_SERIES_NAME, timeSeriesName)
                    .setInt(VERSION, version)
                    .setUuid(CHUNK_ID, chunkId)
                    .setInt(CHUNK_TYPE, chunk.isCompressed() ? TimeSeriesChunkType.DOUBLE_COMPRESSED.ordinal()
                        : TimeSeriesChunkType.DOUBLE_UNCOMPRESSED.ordinal()));

                if (chunk.isCompressed()) {
                    statements.add(preparedStatementsSupplier.get().getInsertDoubleTimeSeriesDataCompressedChunksPreparedStmt()
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
                    statements.add(preparedStatementsSupplier.get().getInsertDoubleTimeSeriesDataUncompressedChunksPreparedStmt()
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
         * <p>Cassandra does not support null values in collection, so in order to avoid the following error, we replace
         * null strings by empty strings.</p>
         *
         * <pre> java.lang.NullPointerException: Collection elements cannot be null
         *
         *     at com.datastax.driver.core.TypeCodec$AbstractCollectionCodec.serialize(TypeCodec.java:1767)
         *     at com.datastax.driver.core.TypeCodec$AbstractCollectionCodec.serialize(TypeCodec.java:1749)
         *     at com.datastax.driver.core.AbstractData.setList(AbstractData.java:358)
         *     at com.datastax.driver.core.BoundStatement.setList(BoundStatement.java:654)
         *     at com.rte_france.powsybl.afs.cassandra.CassandraAppStorage$1.flush(CassandraAppStorage.java:179)}</pre>
         */
        private List<String> fixNullValues(String[] values) {
            Objects.requireNonNull(values);
            return Arrays.stream(values).map(v -> v != null ? v : "").collect(Collectors.toList());
        }

        private void flush(StringTimeSeriesChunksAddition addition, List<Statement<?>> statements, TimeSeriesWritingContext writingContext) {
            UUID nodeUuid = checkNodeId(addition.getNodeId());
            int version = addition.getVersion();
            String timeSeriesName = addition.getTimeSeriesName();
            List<StringDataChunk> chunks = addition.getChunks();

            for (StringDataChunk chunk : chunks) {
                UUID chunkId = Uuids.timeBased();

                // skip empty chunks
                if (chunk.getLength() == 0) {
                    logEmptyChunk(timeSeriesName, version, nodeUuid);
                    continue;
                }

                statements.add(preparedStatementsSupplier.get().getInsertTimeSeriesDataChunksPreparedStmt()
                    .bind()
                    .setUuid(ID, nodeUuid)
                    .setString(TIME_SERIES_NAME, timeSeriesName)
                    .setInt(VERSION, version)
                    .setUuid(CHUNK_ID, chunkId)
                    .setInt(CHUNK_TYPE, chunk.isCompressed() ? TimeSeriesChunkType.STRING_COMPRESSED.ordinal()
                        : TimeSeriesChunkType.STRING_UNCOMPRESSED.ordinal()));

                if (chunk.isCompressed()) {
                    statements.add(preparedStatementsSupplier.get().getInsertStringTimeSeriesDataCompressedChunksPreparedStmt()
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
                    statements.add(preparedStatementsSupplier.get().getInsertStringTimeSeriesDataUncompressedChunksPreparedStmt()
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

            List<Statement<?>> statements = new ArrayList<>();

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
                        throw new AfsException("Unexpected storage change type: " + change.getType());
                }
            }

            // write statements
            Statement<?> statement = null;
            try {
                for (Statement<?> s : statements) {
                    statement = s;
                    getSession().execute(s);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to execute statement {}. The subsequent buffered changes in {} will be ignored.", statement, changeSet);
                throw e;
            }

            watch.stop();

            if (writingContext.createdTimeSeriesCount > 0 || writingContext.insertedChunkCount > 0) {
                LOGGER.info("{} times series created and {} data chunks inserted in {} ms",
                    writingContext.createdTimeSeriesCount, writingContext.insertedChunkCount, watch.elapsed(TimeUnit.MILLISECONDS));
            }
        }

        private void logEmptyChunk(String timeSeriesName, int version, UUID nodeUuid) {
            LOGGER.warn("Empty chunk for time series {} version {} of node {}", timeSeriesName, version, nodeUuid);
        }
    };
    private final StorageChangeBuffer changeBuffer;

    public CassandraAppStorage(String fileSystemName, Supplier<CassandraContext> contextSupplier,
                               CassandraAppStorageConfig config, EventsBus eventsBus) {
        this.eventsBus = eventsBus;
        this.fileSystemName = Objects.requireNonNull(fileSystemName);
        this.contextSupplier = Suppliers.memoize(Objects.requireNonNull(contextSupplier::get));
        this.config = Objects.requireNonNull(config);

        // WARNING: Cassandra cannot mutate more that 16Mo per query!
        changeBuffer = new StorageChangeBuffer(changeFlusher, config.getFlushMaximumChange(), config.getFlushMaximumSize());

        // prepared statement
        preparedStatementsSupplier = Suppliers.memoize(() -> new PreparedStatements(this));
    }

    private static CassandraAfsException createNodeNotFoundException(UUID nodeId) {
        return new CassandraAfsException("Node '" + nodeId + "' not found");
    }

    private static boolean isConsistentBackwardCompatible(Row row, int i) {
        return row.isNull(i) || row.getBoolean(i);
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

    private static Optional<FileSystemCheckIssue> buildExpirationInconsistentIssue(Row row, Instant instant) {
        return Optional.ofNullable(buildExpirationInconsistent(row, instant));
    }

    private static FileSystemCheckIssue buildExpirationInconsistent(Row row, Instant instant) {
        Instant rowInstant = row.getInstant(MODIFICATION_DATE);
        if (rowInstant != null && rowInstant.isBefore(instant) && !row.getBoolean(CONSISTENT)) {
            final FileSystemCheckIssue fileSystemCheckIssue = buildIssue(row);
            if (fileSystemCheckIssue != null) {
                fileSystemCheckIssue.setType("inconsistent");
                fileSystemCheckIssue.setDescription("inconsistent and older than " + instant);
                return fileSystemCheckIssue;
            }
        }
        return null;
    }

    private static FileSystemCheckIssue buildIssue(Row row) {
        UUID uuid = row.getUuid(ID);
        if (uuid == null) {
            return null;
        }
        final FileSystemCheckIssue issue = new FileSystemCheckIssue();
        issue.setNodeId(uuid.toString()).setNodeName(row.getString(NAME));
        return issue;
    }

    @Override
    public String getFileSystemName() {
        return fileSystemName;
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    CqlSession getSession() {
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
            BatchStatementBuilder batchStatements = new BatchStatementBuilder(BatchType.UNLOGGED);
            rootNodeInfo = createNode(new NodeParameters(rootNodeUuid, null, name, nodePseudoClass, "", 0, new NodeGenericMetadata()), batchStatements);
            getSession().execute(batchStatements.build());
            setConsistent(rootNodeInfo.getId());
        } else {
            rootNodeInfo = getNodeInfo(rootNodeUuid);
        }

        return rootNodeInfo;
    }

    private Row getRowByUuid(UUID nodeUuid, String column) {
        Objects.requireNonNull(nodeUuid);
        SimpleStatement selectQuery = selectFrom(CHILDREN_BY_NAME_AND_CLASS)
            .distinct()
            .column(column)
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .build();
        ResultSet resultSet = getSession().execute(selectQuery);
        Row row = resultSet.one();
        if (row == null) {
            throw createNodeNotFoundException(nodeUuid);
        }
        return row;
    }

    private String getNodeName(UUID nodeUuid) {
        Row row = getRowByUuid(nodeUuid, NAME);
        return row.getString(0);
    }

    @Override
    public boolean isWritable(String nodeId) {
        return true;
    }

    @Override
    public boolean isConsistent(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Row row = getRowByUuid(nodeUuid, CONSISTENT);
        return isConsistentBackwardCompatible(row, 0);
    }

    private NodeInfo createNode(UUID parentNodeUuid, String name, String nodePseudoClass, String description,
                                int version, NodeGenericMetadata genericMetadata, BatchStatementBuilder batchStatements) {
        return createNode(new NodeParameters(Uuids.timeBased(), parentNodeUuid, name, nodePseudoClass, description, version, genericMetadata), batchStatements);
    }

    private record NodeParameters(UUID nodeUuid, UUID parentNodeUuid, String name, String nodePseudoClass,
                                  String description, int version, NodeGenericMetadata genericMetadata) { }

    private NodeInfo createNode(NodeParameters nodeParameters,
                                BatchStatementBuilder batchStatements) {
        long creationTime = ZonedDateTime.now().toInstant().toEpochMilli();
        batchStatements.addStatement(insertInto(CHILDREN_BY_NAME_AND_CLASS)
            .value(ID, literal(nodeParameters.nodeUuid))
            .value(NAME, literal(nodeParameters.name))
            .value(PARENT_ID, literal(nodeParameters.parentNodeUuid))
            .value(PSEUDO_CLASS, literal(nodeParameters.nodePseudoClass))
            .value(DESCRIPTION, literal(nodeParameters.description))
            .value(CONSISTENT, literal(false))
            .value(CREATION_DATE, literal(Instant.ofEpochMilli(creationTime)))
            .value(MODIFICATION_DATE, literal(Instant.ofEpochMilli(creationTime)))
            .value(VERSION, literal(nodeParameters.version))
            .values(addAllMetadata(nodeParameters.genericMetadata))
            .build());

        if (nodeParameters.parentNodeUuid != null) {
            batchStatements.addStatement(insertInto(CHILDREN_BY_NAME_AND_CLASS)
                .value(ID, literal(nodeParameters.parentNodeUuid))
                .value(CHILD_NAME, literal(nodeParameters.name))
                .value(CHILD_ID, literal(nodeParameters.nodeUuid))
                .value(CHILD_PSEUDO_CLASS, literal(nodeParameters.nodePseudoClass))
                .value(CHILD_DESCRIPTION, literal(nodeParameters.description))
                .value(CHILD_CONSISTENT, literal(false))
                .value(CHILD_CREATION_DATE, literal(Instant.ofEpochMilli(creationTime)))
                .value(CHILD_MODIFICATION_DATE, literal(Instant.ofEpochMilli(creationTime)))
                .value(CHILD_VERSION, literal(nodeParameters.version))
                .values(addAllChildMetadata(nodeParameters.genericMetadata))
                .build());
        }
        pushEvent(new NodeCreated(nodeParameters.nodeUuid.toString(),
            nodeParameters.parentNodeUuid != null ? nodeParameters.parentNodeUuid.toString() : null), APPSTORAGE_NODE_TOPIC);
        return new NodeInfo(nodeParameters.nodeUuid.toString(), nodeParameters.name, nodeParameters.nodePseudoClass,
            nodeParameters.description, creationTime, creationTime, nodeParameters.version, nodeParameters.genericMetadata);
    }

    @Override
    public NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, int version,
                               NodeGenericMetadata genericMetadata) {
        UUID parentNodeUuid = checkNodeId(parentNodeId);
        Objects.requireNonNull(name);
        Objects.requireNonNull(nodePseudoClass);

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatementBuilder batchStatements = new BatchStatementBuilder(BatchType.UNLOGGED);
        NodeInfo nodeInfo = createNode(parentNodeUuid, name, nodePseudoClass, description, version, genericMetadata, batchStatements);
        getSession().execute(batchStatements.build());
        return nodeInfo;
    }

    @Override
    public void setMetadata(String nodeId, NodeGenericMetadata genericMetadata) {
        UUID nodeUuid = checkNodeId(nodeId);
        NodeGenericMetadata newMetadata = genericMetadata != null ? genericMetadata : new NodeGenericMetadata();

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatementBuilder batchStatements = new BatchStatementBuilder(BatchType.UNLOGGED);
        batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
            .setColumn(MD, literal(newMetadata.getDoubles()))
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .build());
        batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
            .setColumn(MT, literal(newMetadata.getStrings()))
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .build());
        batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
            .setColumn(MI, literal(newMetadata.getInts()))
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .build());
        batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
            .setColumn(MB, literal(newMetadata.getBooleans()))
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .build());

        UUID parentNodeUuid = getParentNodeUuid(nodeUuid);
        if (parentNodeUuid != null) {
            NodeInfo nodeInfo = getNodeInfo(nodeId);
            batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(CMD, literal(newMetadata.getDoubles()))
                .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                .build());
            batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(CMT, literal(newMetadata.getStrings()))
                .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                .build());
            batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(CMI, literal(newMetadata.getInts()))
                .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                .build());
            batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(CMB, literal(newMetadata.getBooleans()))
                .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
                .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                .build());
        }

        getSession().execute(batchStatements.build());
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
                .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
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
                .values(addAllChildMetadata(nodeInfo))
                .build());

            getSession().execute(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(NAME, literal(name))
                .whereColumn(ID).isEqualTo(literal(nodeUuid))
                .build());
        });
        pushEvent(new NodeNameUpdated(nodeId, name), APPSTORAGE_NODE_TOPIC);
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
        Instant creationTime = row.getInstant(3);
        Instant modificationTime = row.getInstant(4);
        if (creationTime == null || modificationTime == null) {
            throw createNodeNotFoundException(nodeUuid);
        }
        return new NodeInfo(nodeUuid.toString(),
            row.getString(0),
            row.getString(1),
            row.getString(2),
            creationTime.toEpochMilli(),
            modificationTime.toEpochMilli(),
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
        BatchStatementBuilder batchStatements = new BatchStatementBuilder(BatchType.UNLOGGED);
        batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
            .setColumn(attributeName, literal(newValue))
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .build());
        if (parentNodeId != null) {
            NodeInfo nodeInfo = getNodeInfo(nodeId);
            batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
                .setColumn(childAttributeName, literal(newValue))
                .whereColumn(ID).isEqualTo(literal(parentNodeId))
                .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
                .whereColumn(CHILD_PSEUDO_CLASS).isEqualTo(literal(nodeInfo.getPseudoClass()))
                .build());
        }
        getSession().execute(batchStatements.build());
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
                addNodeInfo(childNodesInfo, row, uuid);
            }
        }
        return childNodesInfo;
    }

    private void addNodeInfo(List<NodeInfo> childNodesInfo, Row row, UUID uuid) {
        Instant creationTime = row.getInstant(4);
        Instant modificationTime = row.getInstant(5);
        if (creationTime != null && modificationTime != null) {
            NodeInfo nodeInfo = new NodeInfo(uuid.toString(),
                row.getString(0),
                row.getString(1),
                row.getString(3),
                creationTime.toEpochMilli(),
                modificationTime.toEpochMilli(),
                row.getInt(6),
                new NodeGenericMetadata(row.getMap(7, String.class, String.class),
                    row.getMap(8, String.class, Double.class),
                    row.getMap(9, String.class, Integer.class),
                    row.getMap(10, String.class, Boolean.class)));
            childNodesInfo.add(nodeInfo);
        }
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
                addNodeInfo(childNodesInfo, row, uuid);
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
        UUID uuid = row.getUuid(0);
        Instant creationTime = row.getInstant(3);
        Instant modificationTime = row.getInstant(4);
        return uuid == null || creationTime == null || modificationTime == null ?
            Optional.empty() :
            Optional.of(new NodeInfo(uuid.toString(),
                name,
                row.getString(1),
                row.getString(2),
                creationTime.toEpochMilli(),
                modificationTime.toEpochMilli(),
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

        BatchStatementBuilder batchStatements = new BatchStatementBuilder(BatchType.UNLOGGED);
        batchStatements.addStatement(update(CHILDREN_BY_NAME_AND_CLASS)
            .setColumn(PARENT_ID, literal(newParentNodeUuid))
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .build());
        batchStatements.addStatement(deleteFrom(CHILDREN_BY_NAME_AND_CLASS)
            .whereColumn(ID).isEqualTo(literal(currentParentId))
            .whereColumn(CHILD_NAME).isEqualTo(literal(nodeInfo.getName()))
            .build());
        batchStatements.addStatement(insertInto(CHILDREN_BY_NAME_AND_CLASS)
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
        getSession().execute(batchStatements.build());

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

        BatchStatements batchStatements = new BatchStatements(
            () -> new BatchStatementBuilder(BatchType.UNLOGGED),
            builder -> getSession().execute(builder.build()));

        // children
        batchStatements.addStatement(deleteFrom(CHILDREN_BY_NAME_AND_CLASS)
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .build());
        UUID parentNodeUuid = getParentNodeUuid(nodeUuid);
        batchStatements.addStatement(deleteFrom(CHILDREN_BY_NAME_AND_CLASS)
            .whereColumn(ID).isEqualTo(literal(parentNodeUuid))
            .whereColumn(CHILD_NAME).isEqualTo(literal(getNodeName(nodeUuid)))
            .build());

        // data
        removeAllData(nodeUuid, batchStatements);

        // time series
        clearTimeSeries(nodeUuid, batchStatements);

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
            batchStatements.addStatement(deleteFrom(DEPENDENCIES)
                .whereColumn(FROM_ID).isEqualTo(literal(otherNodeUuid))
                .whereColumn(NAME).isEqualTo(literal(name))
                .whereColumn(TO_ID).isEqualTo(literal(nodeUuid))
                .build());
        }

        batchStatements.addStatement(deleteFrom(DEPENDENCIES)
            .whereColumn(FROM_ID)
            .isEqualTo(literal(nodeUuid))
            .build());

        List<Term> listDependencies = dependencies.values().stream()
            .map(QueryBuilder::literal)
            .collect(Collectors.toList());

        if (!listDependencies.isEmpty()) {
            batchStatements.addStatement(deleteFrom(BACKWARD_DEPENDENCIES)
                .whereColumn(TO_ID).in(listDependencies)
                .build());
        }

        batchStatements.execute();

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

        return Optional.of(new BinaryDataInputStream(this, nodeUuid, name, firstRow));
    }

    @Override
    public OutputStream writeBinaryData(String nodeId, String name) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        // flush buffer to keep change order
        changeBuffer.flush();
        return new BinaryDataOutputStream(this, config, nodeUuid, name);
    }

    private boolean dataExists(String nodeId, String name, String tableName, String nameColumn) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        ResultSet resultSet = getSession().execute(selectFrom(tableName)
            .countAll()
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .whereColumn(nameColumn).isEqualTo(literal(name))
            .build());
        Row row = resultSet.one();
        return row != null && row.getLong(0) > 0;
    }

    @Override
    public boolean dataExists(String nodeId, String name) {
        return dataExists(nodeId, name, NODE_DATA_NAMES, NAME);
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

    private void removeData(UUID nodeUuid, String name, BatchStatements batchStatements) {
        batchStatements.addStatement(deleteFrom(NODE_DATA)
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .whereColumn(NAME).isEqualTo(literal(name))
            .build());
        batchStatements.addStatement(deleteFrom(NODE_DATA_NAMES)
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .whereColumn(NAME).isEqualTo(literal(name))
            .build());
        pushEvent(new NodeDataRemoved(nodeUuid.toString(), name), APPSTORAGE_NODE_TOPIC);
    }

    private void removeAllData(UUID nodeUuid, BatchStatements batchStatements) {
        for (String dataName : getDataNames(nodeUuid)) {
            removeData(nodeUuid, dataName, batchStatements);
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

        BatchStatements batchStatements = new BatchStatements(
            () -> new BatchStatementBuilder(BatchType.UNLOGGED),
            builder -> getSession().execute(builder.build())
        );
        removeData(nodeUuid, name, batchStatements);
        batchStatements.execute();

        return true;
    }

    @Override
    public void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        changeBuffer.createTimeSeries(nodeId, metadata);
        pushEvent(new TimeSeriesCreated(nodeId, metadata.getName()),
            APPSTORAGE_TIMESERIES_TOPIC);
    }

    @Override
    public boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        return dataExists(nodeId, timeSeriesName, REGULAR_TIME_SERIES, TIME_SERIES_NAME);
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
            String name = row.getString(0);
            Map<String, String> tags = row.getMap(2, String.class, String.class);
            Instant startTime = row.getInstant(3);
            Instant endTime = row.getInstant(4);
            if (name != null && tags != null && startTime != null && endTime != null) {
                timeSeries.add(new TimeSeriesMetadata(name,
                    TimeSeriesDataType.valueOf(row.getString(1)),
                    tags,
                    new RegularTimeSeriesIndex(startTime.toEpochMilli(),
                        endTime.toEpochMilli(),
                        row.getLong(5))));
            }
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
                if (values != null) {
                    timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new UncompressedDoubleDataChunk(offset, values.stream().mapToDouble(Double::valueOf).toArray()));
                }
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
                if (stepValues != null && stepLengths != null) {
                    timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new CompressedDoubleDataChunk(offset, length,
                            stepValues.stream().mapToDouble(Double::valueOf).toArray(),
                            stepLengths.stream().mapToInt(Integer::valueOf).toArray()));
                }
            }

            // Reorder the elements of the lists by offset
            timeSeriesData.forEach((name, list) -> list.sort(Comparator.comparing(DoubleDataChunk::getOffset)));
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
                if (values != null) {
                    timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new UncompressedStringDataChunk(offset, values.toArray(new String[0])));
                }
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
                if (stepValues != null && stepLengths != null) {
                    timeSeriesData.computeIfAbsent(name, k -> new ArrayList<>())
                        .add(new CompressedStringDataChunk(offset, length,
                            stepValues.toArray(new String[0]),
                            stepLengths.stream().mapToInt(Integer::valueOf).toArray()));
                }
            }

            // Reorder the elements of the lists by offset
            timeSeriesData.forEach((name, list) -> list.sort(Comparator.comparing(StringDataChunk::getOffset)));
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

    private void clearTimeSeries(UUID nodeUuid, BatchStatements batchStatements) {
        batchStatements.addStatement(deleteFrom(REGULAR_TIME_SERIES)
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
                    batchStatements.addStatement(deleteFrom(DOUBLE_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                        .whereColumn(ID).isEqualTo(literal(nodeUuid)).whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                        .whereColumn(VERSION).isEqualTo(literal(version))
                        .build());
                    break;
                case DOUBLE_COMPRESSED:
                    batchStatements.addStatement(deleteFrom(DOUBLE_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                        .whereColumn(ID).isEqualTo(literal(nodeUuid)).whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                        .whereColumn(VERSION).isEqualTo(literal(version))
                        .build());
                    break;
                case STRING_UNCOMPRESSED:
                    batchStatements.addStatement(deleteFrom(STRING_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS)
                        .whereColumn(ID).isEqualTo(literal(nodeUuid)).whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                        .whereColumn(VERSION).isEqualTo(literal(version))
                        .build());
                    break;
                case STRING_COMPRESSED:
                    batchStatements.addStatement(deleteFrom(STRING_TIME_SERIES_DATA_COMPRESSED_CHUNKS)
                        .whereColumn(ID).isEqualTo(literal(nodeUuid)).whereColumn(TIME_SERIES_NAME).isEqualTo(literal(timeSeriesName))
                        .whereColumn(VERSION).isEqualTo(literal(version))
                        .build());
                    break;
                default:
                    throw new AfsException("Unknown chunk type " + chunkType);
            }
        }
        batchStatements.addStatement(deleteFrom(TIME_SERIES_DATA_CHUNK_TYPES)
            .whereColumn(ID).isEqualTo(literal(nodeUuid))
            .build());
    }

    @Override
    public void clearTimeSeries(String nodeId) {
        UUID nodeUuid = checkNodeId(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatements batchStatements = new BatchStatements(
            () -> new BatchStatementBuilder(BatchType.UNLOGGED),
            builder -> getSession().execute(builder.build())
        );
        clearTimeSeries(nodeUuid, batchStatements);
        batchStatements.execute();
        pushEvent(new TimeSeriesCleared(nodeUuid.toString()), APPSTORAGE_TIMESERIES_TOPIC);
    }

    @Override
    public void addDependency(String nodeId, String name, String toNodeId) {
        UUID nodeUuid = checkNodeId(nodeId);
        Objects.requireNonNull(name);
        UUID toNodeUuid = checkNodeId(toNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        BatchStatementBuilder batchStatements = new BatchStatementBuilder(BatchType.UNLOGGED);

        batchStatements.addStatement(insertInto(DEPENDENCIES)
            .value(FROM_ID, literal(nodeUuid))
            .value(NAME, literal(name))
            .value(TO_ID, literal(toNodeUuid))
            .value("dep_id", literal(Uuids.timeBased()))
            .build());
        batchStatements.addStatement(insertInto(BACKWARD_DEPENDENCIES)
            .value(TO_ID, literal(toNodeUuid))
            .value(NAME, literal(name))
            .value(FROM_ID, literal(nodeUuid))
            .build());
        getSession().execute(batchStatements.build());
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

        BatchStatementBuilder batchStatements = new BatchStatementBuilder(BatchType.UNLOGGED);

        batchStatements.addStatement(deleteFrom(DEPENDENCIES)
            .whereColumn(FROM_ID).isEqualTo(literal(nodeUuid))
            .whereColumn(NAME).isEqualTo(literal(name))
            .whereColumn(TO_ID).isEqualTo(literal(toNodeUuid))
            .build());
        batchStatements.addStatement(deleteFrom(BACKWARD_DEPENDENCIES)
            .whereColumn(TO_ID).isEqualTo(literal(toNodeUuid))
            .whereColumn(NAME).isEqualTo(literal(name))
            .whereColumn(FROM_ID).isEqualTo(literal(nodeUuid))
            .build());

        getSession().execute(batchStatements.build());
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
        return List.of(FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES,
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
            BatchStatements batchStatements = new BatchStatements(
                () -> new BatchStatementBuilder(BatchType.UNLOGGED),
                builder -> getSession().execute(builder.build())
            );
            for (UUID id : orphanDataIds) {
                removeAllData(id, batchStatements);
            }
            batchStatements.execute();
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
        for (SimpleStatement statement : statements) {
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

    private void repairExpirationInconsistent(FileSystemCheckIssue issue) {
        deleteNode(issue.getNodeId());
        issue.setRepaired(true);
        issue.setResolutionDescription("deleted");
    }

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

    @Override
    protected void pushEvent(NodeEvent event, String topic) {
        if (getEventsBus() == null) {
            LOGGER.warn("Event can't be pushed : No EventStore instance is available.");
            return;
        }
        getEventsBus().pushEvent(event, topic);
    }
}
