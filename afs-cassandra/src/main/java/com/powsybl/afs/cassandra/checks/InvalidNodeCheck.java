/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra.checks;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.powsybl.afs.cassandra.CassandraConstants;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.powsybl.afs.cassandra.CassandraConstants.*;
import static com.powsybl.afs.cassandra.CassandraConstants.CHILD_NAME;

/**
 * Nodes with null name are considered invalid (name is mandatory in the data model).
 * TODO: we could also check for nullity of other require attributes, such as dates
 *
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class InvalidNodeCheck implements CassandraAppStorageCheck {

    public static final String NAME = "INVALID_NODES";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<FileSystemCheckIssue> check(CassandraAppStorageCheckSupport context, FileSystemCheckOptions options) {
        List<FileSystemCheckIssue> issues = readAllNodes(context.getSession()).stream()
            .filter(node -> node.getName() == null)
            .map(node -> new FileSystemCheckIssue()
                .setType(getName())
                .setNodeId(node.getId().toString())
                .setDescription("Node has no name, which is mandatory")
                .setResolutionDescription("Node will be deleted"))
            .collect(Collectors.toList());

        if (options.isRepair()) {
            issues.forEach(issue -> {
                //cannot use storage.deleteNode
                context.getSession().execute(delete().from(CHILDREN_BY_NAME_AND_CLASS).where(eq(ID, UUID.fromString(issue.getNodeId()))));
                issue.setRepaired(true)
                    .setResolutionDescription("Node has been deleted");
            });
        }
        return issues;
    }


    /**
     * Reads the whole nodes table in memory
     */
    private List<NodeRow> readAllNodes(Session session) {
        List<NodeRow> nodes = new ArrayList<>();
        ResultSet res = session.execute(select(PARENT_ID, ID, CassandraConstants.NAME, CHILD_ID, CHILD_NAME).from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : res) {
            nodes.add(new NodeRow(row.getUUID(0), row.getUUID(1), row.getString(2), row.getUUID(3), row.getString(4)));
        }
        return nodes;
    }


    /**
     * Simple class which embeds raw data for 1 line of the children-by-name-and class, for maintenance purpose.
     */
    static class NodeRow {

        private final UUID parentId;
        private final UUID id;
        private final String name;
        private final UUID childId;
        private final String childName;

        NodeRow(UUID parentId, UUID id, String name, UUID childId, String childName) {
            this.parentId = parentId;
            this.id = id;
            this.name = name;
            this.childId = childId;
            this.childName = childName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(parentId, id, name, childId, childName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NodeRow nodeRow = (NodeRow) o;
            return Objects.equals(parentId, nodeRow.parentId)
                && Objects.equals(id, nodeRow.id)
                && Objects.equals(name, nodeRow.name)
                && Objects.equals(childId, nodeRow.childId)
                && Objects.equals(childName, nodeRow.childName);
        }

        @Override
        public String toString() {
            return "NodeRow{" +
                "parentId=" + parentId +
                ", id=" + id +
                ", name='" + name + '\'' +
                ", childId=" + childId +
                ", childName='" + childName + '\'' +
                '}';
        }

        public UUID getParentId() {
            return parentId;
        }

        public UUID getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public UUID getChildId() {
            return childId;
        }

        public String getChildName() {
            return childName;
        }
    }
}
