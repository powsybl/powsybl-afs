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
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import com.powsybl.afs.storage.check.FileSystemCheckOptionsBuilder;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.CassandraContainer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.powsybl.afs.cassandra.CassandraConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
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

//    @Override
//    @BeforeEach
//    public void setUp() {
//        eventStack = new LinkedBlockingQueue<>();
//        this.storage = createStorage();
//        this.storage.getEventsBus().addListener(l);
//    }

    @AfterEach
    @Override
    public void tearDown() {
        super.tearDown();
        clear();
    }

    @Override
    protected AppStorage createStorage() {
        return new CassandraAppStorage("test", () -> new CassandraTestContext(cassandraSession),
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
        testSupportedChecks();
        testInconsistendNodeRepair();
        testAbsentChildRepair();
        testOrphanNodeRepair();
        testOrphanDataRepair();
        testGetParentWithInconsistentChild();
        clear();

        try {
            new CassandraDataSplit().test(cassandraSession);
            clear();
        } catch (IOException e) {
            fail();
        }

        new CassandraDescriptionIssue().test(storage);
        clear();

        new CassandraLeak().test(storage, cassandraSession);
        clear();

        new CassandraRemoveCreateFolderIssue().test(storage);
        clear();

        new CassandraRenameIssue().test(storage);
        clear();

        new CassandraRenameIssue().testRenameChildWithSameName(storage);
        clear();

        new TimeSeriesIssue().testEmptyChunks(storage);
        clear();
        new TimeSeriesIssue().testNullString(storage);
        clear();
    }

    private void testOrphanDataRepair() {
        NodeInfo rootFolderInfo = storage.createRootNodeIfNotExists(storage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        try (OutputStream os = storage.writeBinaryData(rootFolderInfo.getId(), "should_exist")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            fail();
        }
        String orphanDataId = Uuids.timeBased().toString();
        try (OutputStream os = storage.writeBinaryData(orphanDataId, "blob")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            fail();
        }
        assertThat(storage.getDataNames(orphanDataId)).containsOnly("blob");
        assertAfsNodeNotFound(orphanDataId);

        FileSystemCheckOptions repairOption = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(CassandraAppStorage.ORPHAN_DATA)
            .repair().build();
        List<FileSystemCheckIssue> issues = storage.checkFileSystem(repairOption);
        assertEquals(1,
            issues.stream()
                .filter(issue -> issue.getNodeId().equals(orphanDataId))
                .filter(issue -> issue.getType().equals(CassandraAppStorage.ORPHAN_DATA))
                .filter(issue -> issue.getNodeName().equals("N/A"))
                .count()
        );

        assertTrue(storage.dataExists(rootFolderInfo.getId(), "should_exist"));
        assertFalse(storage.dataExists(orphanDataId, "blob"));
    }

    private void testOrphanNodeRepair() {
        NodeInfo orphanNode = storage.createNode(Uuids.timeBased().toString(), "orphanNodes", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(orphanNode.getId());
        try (OutputStream os = storage.writeBinaryData(orphanNode.getId(), "blob")) {
            os.write("word2".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            fail();
        }
        storage.flush();
        NodeInfo orphanChild = storage.createNode(orphanNode.getId(), "orphanChild", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
        storage.setConsistent(orphanChild.getId());
        FileSystemCheckOptions repairOption = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(CassandraAppStorage.ORPHAN_NODE)
            .repair().build();
        List<FileSystemCheckIssue> issues = storage.checkFileSystem(repairOption);
        assertEquals(1,
            issues.stream()
                .filter(issue -> issue.getNodeId().equals(orphanNode.getId()))
                .count());
        assertAfsNodeNotFound(orphanNode.getId());
        assertAfsNodeNotFound(orphanNode.getId());
        assertAfsNodeNotFound(orphanChild.getId());
        assertThat(storage.getDataNames(orphanNode.getId())).isEmpty();
    }

    private void assertAfsNodeNotFound(String id) {
        assertThrows(CassandraAfsException.class, () -> storage.getNodeInfo(id), "not found");
    }

    void testInconsistendNodeRepair() {
        NodeInfo rootFolderInfo = storage.createRootNodeIfNotExists(storage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        NodeInfo inconsistentNode = storage.createNode(rootFolderInfo.getId(), "inconsistentNode", FOLDER_PSEUDO_CLASS, "", 0,
            new NodeGenericMetadata().setString("k", "v"));
        inconsistentNode.setModificationTime(Instant.now().minus(3, ChronoUnit.DAYS).toEpochMilli());
        storage.flush();

        assertFalse(storage.isConsistent(inconsistentNode.getId()));
        assertEquals(1, storage.getInconsistentNodes().size());
        assertEquals(inconsistentNode.getId(), storage.getInconsistentNodes().get(0).getId());
        storage.flush();
        final FileSystemCheckOptions dryRunOptions = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES)
            // normal should use minus to check filesystem, but here we could not set modification time to an earlier time
            .setInconsistentNodesExpirationTime(Instant.now().plus(2, ChronoUnit.DAYS))
            .dryRun().build();
        final List<FileSystemCheckIssue> fileSystemCheckIssues = storage.checkFileSystem(dryRunOptions);
        assertEquals(1, fileSystemCheckIssues.size());
        final FileSystemCheckIssue issue = fileSystemCheckIssues.get(0);
        assertEquals(inconsistentNode.getId(), issue.getNodeId());
        assertEquals("inconsistentNode", issue.getNodeName());
        assertEquals("inconsistent", issue.getType());
        assertFalse(issue.isRepaired());
        assertNotNull(storage.getNodeInfo(inconsistentNode.getId()));

        final FileSystemCheckOptions repairOption = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES)
            .setInconsistentNodesExpirationTime(Instant.now().plus(2, ChronoUnit.DAYS))
            .repair().build();
        final List<FileSystemCheckIssue> repairIssue = storage.checkFileSystem(repairOption);
        assertTrue(repairIssue.get(0).isRepaired());
        String inconsistentNodeId = inconsistentNode.getId();
        assertThrows(CassandraAfsException.class, () -> storage.getNodeInfo(inconsistentNodeId));
    }

    void testAbsentChildRepair() {
        NodeInfo root = storage.createRootNodeIfNotExists(storage.getFileSystemName(), FOLDER_PSEUDO_CLASS);

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
        NodeInfo absentChild = storage.getChildNodes(root.getId()).stream()
            .filter(nodeInfo -> nodeInfo.getName().equals("absent_child"))
            .findFirst().orElseThrow(AssertionError::new);

        String absentChildId = absentChild.getId();
        assertThrows(CassandraAfsException.class, () -> storage.getNodeInfo(absentChildId));

        FileSystemCheckOptions noRepair = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(CassandraAppStorage.REF_NOT_FOUND)
            .build();

        assertEquals(1,
            storage.checkFileSystem(noRepair).stream()
                .filter(issue -> issue.getType().equals(CassandraAppStorage.REF_NOT_FOUND))
                .filter(issue -> issue.getNodeName().equals("absent_child"))
                .filter(issue -> !issue.isRepaired())
                .count()
        );

        //Check again, should still be here
        assertTrue(storage.getChildNodes(root.getId()).stream()
            .anyMatch(nodeInfo -> nodeInfo.getName().equals("absent_child")));

        FileSystemCheckOptions repair = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(CassandraAppStorage.REF_NOT_FOUND)
            .repair()
            .build();

        storage.checkFileSystem(repair);

        //Check again, the wrong child should not be here anymore
        assertFalse(storage.getChildNodes(root.getId()).stream()
            .anyMatch(nodeInfo -> nodeInfo.getName().equals("absent_child")));
    }

    void testSupportedChecks() {
        assertThat(storage.getSupportedFileSystemChecks())
            .containsExactlyInAnyOrder(CassandraAppStorage.REF_NOT_FOUND,
                FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES,
                CassandraAppStorage.ORPHAN_NODE,
                CassandraAppStorage.ORPHAN_DATA);
    }

    private NodeInfo createFolder(NodeInfo parent, String name) {
        return storage.createNode(parent.getId(), name, FOLDER_PSEUDO_CLASS, "", 0,
            new NodeGenericMetadata());
    }

    //Test for bugfix where an inconsistent child was "hiding" the parent of the node
    void testGetParentWithInconsistentChild() {
        NodeInfo root = storage.createRootNodeIfNotExists(storage.getFileSystemName(), FOLDER_PSEUDO_CLASS);
        NodeInfo rootChild = createFolder(root, "rootChild");
        storage.setConsistent(rootChild.getId());
        createFolder(rootChild, "childChild");
        assertThat(storage.getParentNode(rootChild.getId()))
            .hasValueSatisfying(parent -> {
                assertEquals(root.getId(), parent.getId());
            });
    }
}
