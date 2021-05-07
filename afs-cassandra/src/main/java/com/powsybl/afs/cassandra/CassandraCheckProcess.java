/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.powsybl.afs.cassandra.CassandraConstants.*;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class CassandraCheckProcess {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCheckProcess.class);

    private static final String REF_NOT_FOUND = "reference_not_found";

    private final CassandraAppStorage appStorage;
    private final Session session;

    Set<UUID> validUuids = new HashSet<>();
    Set<UUID> invalidUuids = new HashSet<>();

    Set<UUID> checkingFileSystemPrimaryKeys;

    public CassandraCheckProcess(CassandraAppStorage appStorage, Session session, UUID rootUuid) {
        this.appStorage = appStorage;
        this.session = session;
        validUuids.add(rootUuid);
    }

    List<FileSystemCheckIssue> check(FileSystemCheckOptions options) {
        List<FileSystemCheckIssue> results = new ArrayList<>();
        if (options.getInconsistentNodesExpirationTime().isPresent()) {
            checkInconsistent(results, options, validUuids);
        }
        for (String type : options.getTypes()) {
            if (Objects.equals(REF_NOT_FOUND, type)) {
                checkReferenceNotFound(results, options);
            } else {
                LOGGER.warn("Check {} not supported in {}", type, getClass());
            }
        }
        return results;
    }

    private void checkReferenceNotFound(List<FileSystemCheckIssue> results, FileSystemCheckOptions options) {
        List<Statement> statements = new ArrayList<>();
        Set<ChildNodeInfo> notFoundIds = new HashSet<>();
        if (checkingFileSystemPrimaryKeys == null) {
            calculatingAllPrimaryKeyForCheckingFileSystem();
        }
        for (ChildNodeInfo entity : getAllIdsInChildId()) {
            if (!checkingFileSystemPrimaryKeys.contains(entity.id)) {
                notFoundIds.add(entity);
            }
        }
        for (ChildNodeInfo childNodeInfo : notFoundIds) {
            final UUID childId = childNodeInfo.id;
            if (!isNodeInFileSystem(childNodeInfo.parentId, new HashSet<>())) {
                continue;
            }
            final FileSystemCheckIssue issue = new FileSystemCheckIssue()
                    .setUuid(childId)
                    .setName(childNodeInfo.name)
                    .setRepaired(options.isRepair())
                    .setDescription("row is not found but still referenced in " + childNodeInfo.parentId)
                    .setType(REF_NOT_FOUND);
            results.add(issue);
            if (options.isRepair()) {
                statements.add(delete().from(CHILDREN_BY_NAME_AND_CLASS)
                        .where(eq(ID, childNodeInfo.parentId))
                        .and(eq(CHILD_NAME, childNodeInfo.name)));
                issue.setResolutionDescription("reset null child_name and child_id in " + childNodeInfo.parentId);
            }
        }
        if (options.isRepair()) {
            for (Statement statement : statements) {
                session.execute(statement);
            }
        }
    }

    private Set<ChildNodeInfo> getAllIdsInChildId() {
        Set<ChildNodeInfo> set = new HashSet<>();
        ResultSet resultSet = session.execute(select(CHILD_ID, CHILD_NAME, ID).from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : resultSet) {
            final UUID id = row.getUUID(CHILD_ID);
            if (id != null && checkingFileSystemPrimaryKeys.contains(row.getUUID(ID))) {
                set.add(new ChildNodeInfo(id, row.getString(CHILD_NAME), row.getUUID(ID)));
            }
        }
        return set;
    }

    private void calculatingAllPrimaryKeyForCheckingFileSystem() {
        checkingFileSystemPrimaryKeys = new HashSet<>();
        ResultSet resultSet = session.execute(select(ID).distinct().from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : resultSet) {
            UUID uuid = row.getUUID(0);
            if (isNodeInFileSystem(uuid, new HashSet<>())) {
                checkingFileSystemPrimaryKeys.add(uuid);
                System.out.println("add pk:" + uuid);
            }
        }
    }

    private void checkInconsistent(List<FileSystemCheckIssue> results, FileSystemCheckOptions options, Set<UUID> cache) {
        if (checkingFileSystemPrimaryKeys == null) {
            calculatingAllPrimaryKeyForCheckingFileSystem();
        }
        ResultSet resultSet = session.execute(select(ID, NAME, MODIFICATION_DATE, CONSISTENT)
                .from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : resultSet) {
            if (!checkingFileSystemPrimaryKeys.contains(row.getUUID(0))) {
                System.out.println(row.getUUID(0));
                continue;
            }
            final Optional<FileSystemCheckIssue> issue = buildExpirationInconsistentIssue(row, options.getInconsistentNodesExpirationTime().get());
            issue.ifPresent(results::add);
        }
        if (options.isRepair()) {
            for (FileSystemCheckIssue issue : results) {
                if (Objects.equals(issue.getType(), "inconsistent")) {
                    repairExpirationInconsistent(issue);
                }
            }
        }
    }

    private boolean isNodeInFileSystem(UUID id, Set<UUID> paths) {
        if (validUuids.contains(id)) {
            validUuids.addAll(paths);
            validUuids.add(id);
            return true;
        }
        if (invalidUuids.contains(id)) {
            invalidUuids.addAll(paths);
            invalidUuids.add(id);
            return false;
        }

        final Optional<NodeInfo> parentNode = appStorage.getParentNode(id.toString());
        if (parentNode.isPresent()) {
            final UUID parentId = UUID.fromString(parentNode.get().getId());
            paths.add(id);
            return isNodeInFileSystem(parentId, paths);
        } else {
            if (validUuids.contains(id)) {
                validUuids.addAll(paths);
                validUuids.add(id);
                return true;
            }
            if (invalidUuids.contains(id)) {
                invalidUuids.addAll(paths);
                invalidUuids.add(id);
                return false;
            }
            invalidUuids.add(id);
            return false;
        }
    }

    private static Optional<FileSystemCheckIssue> buildExpirationInconsistentIssue(Row row, Instant instant) {
        return Optional.ofNullable(buildExpirationInconsistent(row,  instant));
    }

    private static FileSystemCheckIssue buildExpirationInconsistent(Row row, Instant instant) {
        if (row.getTimestamp(MODIFICATION_DATE).toInstant().isBefore(instant) && !row.getBool(CONSISTENT)) {
            final FileSystemCheckIssue fileSystemCheckIssue = buildIssue(row);
            fileSystemCheckIssue.setType("inconsistent");
            fileSystemCheckIssue.setDescription("inconsistent and older than " + instant);
            return fileSystemCheckIssue;
        }
        return null;
    }

    private static FileSystemCheckIssue buildIssue(Row row) {
        final FileSystemCheckIssue issue = new FileSystemCheckIssue();
        issue.setUuid(row.getUUID(ID)).setName(row.getString(NAME));
        return issue;
    }

    private void repairExpirationInconsistent(FileSystemCheckIssue issue) {
        appStorage.deleteNode(issue.getUuid().toString());
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
