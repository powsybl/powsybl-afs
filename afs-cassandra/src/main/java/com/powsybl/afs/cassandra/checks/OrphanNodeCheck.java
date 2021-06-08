package com.powsybl.afs.cassandra.checks;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.powsybl.afs.cassandra.CassandraConstants;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;

import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.powsybl.afs.cassandra.CassandraConstants.*;
import static com.powsybl.afs.cassandra.CassandraConstants.ID;

/**
 * Identifies and deletes AFS nodes which have a null or non existing parent.
 *
 * @author Yichen TANG <yichen.tang at rte-france.com>
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class OrphanNodeCheck implements CassandraAppStorageCheck {

    public static final String NAME = "ORPHAN_NODE";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<FileSystemCheckIssue> check(CassandraAppStorageCheckSupport context, FileSystemCheckOptions options) {

        List<FileSystemCheckIssue> issues = new ArrayList<>();
        //We may have:
        // nodes with "parent ID" which does not exist
        // nodes with null parent ID and which is not root ?
        // children nodes with "invalid" parent data :

        //Invalid data : ID which does not exist, ID null

        // get all child id which parent name is null
        ResultSet resultSet = context.getSession().execute(select(ID, CHILD_ID, CassandraConstants.NAME, CHILD_NAME).from(CHILDREN_BY_NAME_AND_CLASS));
        List<UUID> orphanIds = new ArrayList<>();
        Set<UUID> fakeParentIds = new HashSet<>();
        for (Row row : resultSet) {
            if (row.getString(CassandraConstants.NAME) == null) {
                UUID nodeId = row.getUUID(CHILD_ID);
                String nodeName = row.getString(CHILD_NAME);
                UUID fakeParentId = row.getUUID(ID);
                FileSystemCheckIssue issue = new FileSystemCheckIssue().setNodeId(nodeId.toString())
                    .setNodeName(nodeName)
                    .setType(getName())
                    .setDescription(nodeName + "(" + nodeId + ") is an orphan node. Its fake parent id=" + fakeParentId);
                if (options.isRepair()) {
                    orphanIds.add(nodeId);
                    fakeParentIds.add(fakeParentId);
                    issue.setRepaired(true);
                    issue.setResolutionDescription("Deleted node [name=" + nodeName + ", id=" + nodeId + "] and reference to null name node [id=" + fakeParentId + "]");
                }
                issues.add(issue);
            }
        }
        if (options.isRepair()) {
            orphanIds.stream().map(UUID::toString).forEach(context.getStorage()::deleteNode);
            for (UUID fakeParentId : fakeParentIds) {
                context.getSession().execute(delete().from(CHILDREN_BY_NAME_AND_CLASS)
                    .where(eq(ID, fakeParentId)));
            }
        }
        return issues;
    }
}
