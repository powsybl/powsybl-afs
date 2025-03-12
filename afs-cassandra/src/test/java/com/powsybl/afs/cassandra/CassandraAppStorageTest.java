/*
 * Copyright (c) 2019-2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.AfsNodeNotFoundException;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import com.powsybl.afs.storage.check.FileSystemCheckOptionsBuilder;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.CassandraContainer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.powsybl.afs.cassandra.CassandraConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class CassandraAppStorageTest extends AbstractAppStorageTest {

    private static CassandraContainer<?> cassandra;
    private static CqlSession cassandraSession;

    @BeforeAll
    static void dontRunOnWindows() {
        assumeFalse(SystemUtils.IS_OS_WINDOWS);
    }

    @BeforeAll
    static void setUpCassandra() {
        // Create container
        cassandra = new CassandraContainer<>("cassandra:3.11.5")
            .withInitScript("afs.cql");

        // Start the container
        cassandra.start();

        // Connect to cassandra
        cassandraSession = CqlSession.builder()
            .addContactPoint(cassandra.getContactPoint())
            .withLocalDatacenter(cassandra.getLocalDatacenter())
            .withKeyspace("afs")
            .build();
    }

    @AfterAll
    static void tearDownCassandra() {
        if (cassandraSession != null) {
            cassandraSession.close();
            if (cassandra != null) {
                cassandra.stop();
            }
        }
    }

    @Override
    protected AppStorage createStorage() {
        return createStorage("test");
    }

    @Override
    protected AppStorage createStorage(String fileSystemName) {
        return new CassandraAppStorage(fileSystemName, () -> new CassandraTestContext(cassandraSession),
            new CassandraAppStorageConfig(), new InMemoryEventsBus());
    }

    /**
     * Clears tables and event stack between tests
     */
    protected void clear() {
        Map<CqlIdentifier, TableMetadata> tables = cassandraSession
            .getMetadata()
            .getKeyspace(AFS_KEYSPACE)
            .orElseThrow(AssertionError::new)
            .getTables();
        tables.keySet().forEach(table -> cassandraSession.execute(QueryBuilder.truncate(table).build()));

        eventStack.clear();
    }

    //Most tests in here to minimize test execution time (only initialize cassandra once)
    @Override
    protected void nextDependentTests() {
        try (AppStorage cassandraAppStorage = createStorage("cassandra-tests")) {
            testSupportedChecks(cassandraAppStorage);
            testInconsistendNodeRepair(cassandraAppStorage);
            testAbsentChildRepair(cassandraAppStorage);
            testOrphanNodeRepair(cassandraAppStorage);
            testOrphanDataRepair(cassandraAppStorage);
            testGetParentWithInconsistentChild(cassandraAppStorage);
            clear();

            try {
                new CassandraDataSplit().test(cassandraSession);
                clear();
            } catch (IOException e) {
                fail();
            }

            new CassandraDescriptionIssue().test(cassandraAppStorage);
            clear();

            new CassandraLeak().test(cassandraAppStorage, cassandraSession);
            clear();

            new CassandraRemoveCreateFolderIssue().test(cassandraAppStorage);
            clear();

            new CassandraRenameIssue().test(cassandraAppStorage);
            clear();

            new CassandraRenameIssue().testRenameChildWithSameName(cassandraAppStorage);
            clear();

            new TimeSeriesIssue().testEmptyChunks(cassandraAppStorage);
            clear();
            new TimeSeriesIssue().testNullString(cassandraAppStorage);
            clear();

        }
    }

    private void testOrphanDataRepair(AppStorage cassandraAppStorage) {
        NodeInfo rootFolderInfo = cassandraAppStorage.createRootNodeIfNotExists(cassandraAppStorage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        try (OutputStream os = cassandraAppStorage.writeBinaryData(rootFolderInfo.getId(), "should_exist")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            fail();
        }
        String orphanDataId = Uuids.timeBased().toString();
        try (OutputStream os = cassandraAppStorage.writeBinaryData(orphanDataId, "blob")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            fail();
        }
        assertThat(cassandraAppStorage.getDataNames(orphanDataId)).containsOnly("blob");
        assertAfsNodeNotFound(cassandraAppStorage, orphanDataId);

        FileSystemCheckOptions repairOption = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(CassandraAppStorage.ORPHAN_DATA)
            .repair().build();
        List<FileSystemCheckIssue> issues = cassandraAppStorage.checkFileSystem(repairOption);
        assertEquals(1, issues.stream()
            .filter(issue -> issue.getNodeId().equals(orphanDataId))
            .filter(issue -> issue.getType().equals(CassandraAppStorage.ORPHAN_DATA))
            .filter(issue -> issue.getNodeName().equals("N/A"))
            .count()
        );

        assertTrue(cassandraAppStorage.dataExists(rootFolderInfo.getId(), "should_exist"));
        assertFalse(cassandraAppStorage.dataExists(orphanDataId, "blob"));
    }

    private void testOrphanNodeRepair(AppStorage cassandraAppStorage) {
        NodeInfo orphanNode = cassandraAppStorage.createNode(Uuids.timeBased().toString(), "orphanNodes", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        cassandraAppStorage.setConsistent(orphanNode.getId());
        try (OutputStream os = cassandraAppStorage.writeBinaryData(orphanNode.getId(), "blob")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            fail();
        }
        cassandraAppStorage.flush();
        NodeInfo orphanChild = cassandraAppStorage.createNode(orphanNode.getId(), "orphanChild", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        cassandraAppStorage.setConsistent(orphanChild.getId());
        FileSystemCheckOptions repairOption = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(CassandraAppStorage.ORPHAN_NODE)
            .repair().build();
        List<FileSystemCheckIssue> issues = cassandraAppStorage.checkFileSystem(repairOption);
        assertEquals(1, issues.stream()
            .filter(issue -> issue.getNodeId().equals(orphanNode.getId()))
            .count());
        assertAfsNodeNotFound(cassandraAppStorage, orphanNode.getId());
        assertAfsNodeNotFound(cassandraAppStorage, orphanNode.getId());
        assertAfsNodeNotFound(cassandraAppStorage, orphanChild.getId());
        assertThat(cassandraAppStorage.getDataNames(orphanNode.getId())).isEmpty();
    }

    private void assertAfsNodeNotFound(AppStorage cassandraAppStorage, String id) {
        assertThrows(AfsNodeNotFoundException.class, () -> cassandraAppStorage.getNodeInfo(id), "not found");
    }

    void testInconsistendNodeRepair(AppStorage cassandraAppStorage) {
        NodeInfo rootFolderInfo = cassandraAppStorage.createRootNodeIfNotExists(cassandraAppStorage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        NodeInfo inconsistentNode = cassandraAppStorage.createNode(rootFolderInfo.getId(), "inconsistentNode", FOLDER_PSEUDO_CLASS, "", 0,
            new NodeGenericMetadata().setString("k", "v"));
        inconsistentNode.setModificationTime(Instant.now().minus(3, ChronoUnit.DAYS).toEpochMilli());
        cassandraAppStorage.flush();

        assertFalse(cassandraAppStorage.isConsistent(inconsistentNode.getId()));
        assertEquals(1, cassandraAppStorage.getInconsistentNodes().size());
        assertEquals(inconsistentNode.getId(), cassandraAppStorage.getInconsistentNodes().get(0).getId());
        cassandraAppStorage.flush();
        final FileSystemCheckOptions dryRunOptions = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES)
            // normal should use minus to check filesystem, but here we could not set modification time to an earlier time
            .setInconsistentNodesExpirationTime(Instant.now().plus(2, ChronoUnit.DAYS))
            .dryRun().build();
        final List<FileSystemCheckIssue> fileSystemCheckIssues = cassandraAppStorage.checkFileSystem(dryRunOptions);
        assertEquals(1, fileSystemCheckIssues.size());
        final FileSystemCheckIssue issue = fileSystemCheckIssues.get(0);
        assertEquals(inconsistentNode.getId(), issue.getNodeId());
        assertEquals("inconsistentNode", issue.getNodeName());
        assertEquals("inconsistent", issue.getType());
        assertFalse(issue.isRepaired());
        assertNotNull(cassandraAppStorage.getNodeInfo(inconsistentNode.getId()));

        final FileSystemCheckOptions repairOption = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES)
            .setInconsistentNodesExpirationTime(Instant.now().plus(2, ChronoUnit.DAYS))
            .repair().build();
        final List<FileSystemCheckIssue> repairIssue = cassandraAppStorage.checkFileSystem(repairOption);
        assertTrue(repairIssue.get(0).isRepaired());
        String inconsistentNodeId = inconsistentNode.getId();
        assertThrows(AfsNodeNotFoundException.class, () -> cassandraAppStorage.getNodeInfo(inconsistentNodeId));
    }

    void testAbsentChildRepair(AppStorage cassandraAppStorage) {
        NodeInfo root = cassandraAppStorage.createRootNodeIfNotExists(cassandraAppStorage.getFileSystemName(), FOLDER_PSEUDO_CLASS);

        // add a child which does not exist to root folder
        SimpleStatement statement = insertInto(CHILDREN_BY_NAME_AND_CLASS)
            .value(ID, literal(UUID.fromString(root.getId())))
            .value(CHILD_NAME, literal("absent_child"))
            .value(CHILD_ID, literal(Uuids.timeBased()))
            .value(CHILD_PSEUDO_CLASS, literal(FOLDER_PSEUDO_CLASS))
            .value(CHILD_DESCRIPTION, literal(""))
            .value(CHILD_CONSISTENT, literal(true))
            .value(CHILD_CREATION_DATE, literal(Instant.now()))
            .value(CHILD_MODIFICATION_DATE, literal(Instant.now()))
            .value(CHILD_VERSION, literal(1))
            .value(CMT, literal(Collections.emptyMap()))
            .value(CMD, literal(Collections.emptyMap()))
            .value(CMI, literal(Collections.emptyMap()))
            .value(CMB, literal(Collections.emptyMap()))
            .build();
        if (cassandraSession != null) {
            cassandraSession.execute(statement);
        }

        //Now root has one child, but that child does not exist
        NodeInfo absentChild = cassandraAppStorage.getChildNodes(root.getId()).stream()
            .filter(nodeInfo -> nodeInfo.getName().equals("absent_child"))
            .findFirst().orElseThrow(AssertionError::new);

        String absentChildId = absentChild.getId();
        assertThrows(AfsNodeNotFoundException.class, () -> cassandraAppStorage.getNodeInfo(absentChildId));

        FileSystemCheckOptions noRepair = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(CassandraAppStorage.REF_NOT_FOUND)
            .build();

        assertEquals(1, cassandraAppStorage.checkFileSystem(noRepair).stream()
            .filter(issue -> issue.getType().equals(CassandraAppStorage.REF_NOT_FOUND))
            .filter(issue -> issue.getNodeName().equals("absent_child"))
            .filter(issue -> !issue.isRepaired())
            .count()
        );

        //Check again, should still be here
        assertTrue(cassandraAppStorage.getChildNodes(root.getId()).stream()
            .anyMatch(nodeInfo -> nodeInfo.getName().equals("absent_child")));

        FileSystemCheckOptions repair = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(CassandraAppStorage.REF_NOT_FOUND)
            .repair()
            .build();

        cassandraAppStorage.checkFileSystem(repair);

        //Check again, the wrong child should not be here anymore
        assertFalse(cassandraAppStorage.getChildNodes(root.getId()).stream()
            .anyMatch(nodeInfo -> nodeInfo.getName().equals("absent_child")));
    }

    void testSupportedChecks(AppStorage cassandraAppStorage) {
        assertThat(cassandraAppStorage.getSupportedFileSystemChecks()).containsExactlyInAnyOrder(
            CassandraAppStorage.REF_NOT_FOUND,
            FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES,
            CassandraAppStorage.ORPHAN_NODE,
            CassandraAppStorage.ORPHAN_DATA);
    }

    private NodeInfo createFolder(AppStorage cassandraAppStorage, NodeInfo parent, String name) {
        return cassandraAppStorage.createNode(parent.getId(), name, FOLDER_PSEUDO_CLASS, "", 0,
            new NodeGenericMetadata());
    }

    //Test for bugfix where an inconsistent child was "hiding" the parent of the node
    void testGetParentWithInconsistentChild(AppStorage cassandraAppStorage) {
        NodeInfo root = cassandraAppStorage.createRootNodeIfNotExists(cassandraAppStorage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        NodeInfo rootChild = createFolder(cassandraAppStorage, root, "rootChild");
        cassandraAppStorage.setConsistent(rootChild.getId());
        createFolder(cassandraAppStorage, rootChild, "childChild");
        assertThat(cassandraAppStorage.getParentNode(rootChild.getId()))
            .hasValueSatisfying(parent -> assertEquals(root.getId(), parent.getId()));
    }
}
