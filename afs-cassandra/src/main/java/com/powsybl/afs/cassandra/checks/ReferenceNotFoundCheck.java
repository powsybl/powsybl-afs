package com.powsybl.afs.cassandra.checks;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;

import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.powsybl.afs.cassandra.CassandraConstants.*;

/**
 * Identifies and deletes references to children nodes which don't exist.
 *
 * @author Yichen TANG <yichen.tang at rte-france.com>
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class ReferenceNotFoundCheck implements CassandraAppStorageCheck {

    public static final String NAME = "REFERENCE_NOT_FOUND";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<FileSystemCheckIssue> check(CassandraAppStorageCheckSupport context, FileSystemCheckOptions options) {
        List<FileSystemCheckIssue> issues = new ArrayList<>();

        List<Statement> statements = new ArrayList<>();
        Set<ChildNodeInfo> notFoundIds = new HashSet<>();
        Set<UUID> existingRows = allPrimaryKeys(context.getSession());
        for (ChildNodeInfo entity : getAllIdsInChildId(context.getSession())) {
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
                .setType(getName());
            issues.add(issue);
            if (options.isRepair()) {
                statements.add(delete().from(CHILDREN_BY_NAME_AND_CLASS)
                    .where(eq(ID, childNodeInfo.parentId))
                    .and(eq(CHILD_NAME, childNodeInfo.name)));
                issue.setResolutionDescription("reset null child_name and child_id in " + childNodeInfo.parentId);
            }
        }
        if (options.isRepair()) {
            statements.forEach(context.getSession()::execute);
        }
        return issues;
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

    private Set<ChildNodeInfo> getAllIdsInChildId(Session session) {
        Set<ChildNodeInfo> set = new HashSet<>();
        ResultSet resultSet = session.execute(select(CHILD_ID, CHILD_NAME, ID).from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : resultSet) {
            final UUID id = row.getUUID(CHILD_ID);
            if (id != null) {
                set.add(new ChildNodeInfo(id, row.getString(CHILD_NAME), row.getUUID(ID)));
            }
        }
        return set;
    }

    private Set<UUID> allPrimaryKeys(Session session) {
        Set<UUID> set = new HashSet<>();
        ResultSet resultSet = session.execute(select(ID).distinct().from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : resultSet) {
            set.add(row.getUUID(0));
        }
        return set;
    }

}
