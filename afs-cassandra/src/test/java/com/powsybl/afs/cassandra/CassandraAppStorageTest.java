/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import com.powsybl.afs.storage.check.FileSystemCheckOptionsBuilder;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

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
                // normal should use minus to check filesystem, but here we could not set modification time to an earlier time
                .setInconsistentNodesExpirationTime(Instant.now().plus(2, ChronoUnit.DAYS))
                .dryRun().build();
        final List<FileSystemCheckIssue> fileSystemCheckIssues = storage.checkFileSystem(dryRunOptions);
        assertEquals(1, fileSystemCheckIssues.size());
        final FileSystemCheckIssue issue = fileSystemCheckIssues.get(0);
        assertEquals(inconsistentNode.getId(), issue.getUuid().toString());
        assertEquals("inconsistentNode", issue.getName());
        assertEquals(FileSystemCheckIssue.Type.EXPIRATION_INCONSISTENT, issue.getType());
        assertFalse(issue.isRepaired());
        assertNotNull(storage.getNodeInfo(inconsistentNode.getId()));

        final FileSystemCheckOptions repairOption = new FileSystemCheckOptionsBuilder()
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
}
