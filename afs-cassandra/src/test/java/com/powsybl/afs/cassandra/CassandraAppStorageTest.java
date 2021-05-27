/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import com.powsybl.afs.storage.check.FileSystemCheckOptionsBuilder;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.powsybl.afs.cassandra.CassandraConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.fail;
import static org.junit.Assert.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraAppStorageTest extends AbstractAppStorageTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("afs.cql", CassandraConstants.AFS_KEYSPACE), null, 20000L);

    @Override
    protected AppStorage createStorage() {
        return new CassandraAppStorage("test", () -> new CassandraTestContext(cassandraCQLUnit),
                new CassandraAppStorageConfig(), new InMemoryEventsBus());
    }

    @Override
    protected void nextDependentTests() {
        testSupportedChecks();
        testInconsistendNodeRepair();
        testAbsentChildRepair();
        testOrphanNodeRepair();
    }

    private void testOrphanNodeRepair() {
        NodeInfo orphanNode = storage.createNode(UUIDs.timeBased().toString(), "orphanNodes", FOLDER_PSEUDO_CLASS, "", 0, new NodeGenericMetadata());
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
        assertThat(issues).hasOnlyOneElementSatisfying(i -> assertEquals(orphanNode.getId(), i.getNodeId()));
        assertThatThrownBy(() -> storage.getNodeInfo(orphanNode.getId()))
                .isInstanceOf(CassandraAfsException.class)
                .hasMessageContaining("not found");
        assertThatThrownBy(() -> storage.getParentNode(orphanNode.getId()))
                .isInstanceOf(CassandraAfsException.class)
                .hasMessageContaining("not found");
        assertThatThrownBy(() -> storage.getNodeInfo(orphanChild.getId()))
                .isInstanceOf(CassandraAfsException.class)
                .hasMessageContaining("not found");
        assertThat(storage.getDataNames(orphanNode.getId())).isEmpty();
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
        assertEquals(inconsistentNode.getId(), issue.getNodeId().toString());
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
        // check deleted
        try {
            storage.getNodeInfo(inconsistentNode.getId());
            fail();
        } catch (CassandraAfsException e) {
            // ignored
        }
    }

    void testAbsentChildRepair() {
        NodeInfo root = storage.createRootNodeIfNotExists(storage.getFileSystemName(), FOLDER_PSEUDO_CLASS);

        // add a child which does not exist to root folder
        Statement statement = insertInto(CHILDREN_BY_NAME_AND_CLASS)
            .value(ID, UUID.fromString(root.getId()))
            .value(CHILD_NAME, "absent_child")
            .value(CHILD_ID, UUIDs.timeBased())
            .value(CHILD_PSEUDO_CLASS, FOLDER_PSEUDO_CLASS)
            .value(CHILD_DESCRIPTION, "")
            .value(CHILD_CONSISTENT, true)
            .value(CHILD_CREATION_DATE, new Date())
            .value(CHILD_MODIFICATION_DATE, new Date())
            .value(CHILD_VERSION, 1)
            .value(CMT, Collections.emptyMap())
            .value(CMD, Collections.emptyMap())
            .value(CMI, Collections.emptyMap())
            .value(CMB, Collections.emptyMap());
        cassandraCQLUnit.getSession().execute(statement);

        //Now root has one child, but that child does not exist
        NodeInfo absentChild = storage.getChildNodes(root.getId()).stream()
            .filter(nodeInfo -> nodeInfo.getName().equals("absent_child"))
            .findFirst().orElseThrow(AssertionError::new);

        assertThatExceptionOfType(CassandraAfsException.class)
            .isThrownBy(() -> storage.getNodeInfo(absentChild.getId()));

        FileSystemCheckOptions noRepair = new FileSystemCheckOptionsBuilder()
            .addCheckTypes(CassandraAppStorage.REF_NOT_FOUND)
            .build();

        assertThat(storage.checkFileSystem(noRepair))
            .hasOnlyOneElementSatisfying(issue -> {
                assertEquals(CassandraAppStorage.REF_NOT_FOUND, issue.getType());
                assertEquals("absent_child", issue.getNodeName());
                assertFalse(issue.isRepaired());
            });

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
            .containsExactlyInAnyOrder(CassandraAppStorage.REF_NOT_FOUND, FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES, CassandraAppStorage.ORPHAN_NODE);
    }
}
