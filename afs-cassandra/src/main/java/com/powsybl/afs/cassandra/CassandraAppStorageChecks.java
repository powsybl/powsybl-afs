package com.powsybl.afs.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.powsybl.afs.cassandra.CassandraConstants.*;
import static com.powsybl.afs.cassandra.CassandraConstants.CHILDREN_BY_NAME_AND_CLASS;

/**
 *
 * Diagnosis and repair tools for maintenance purpose.
 *
 * @author Yichen TANG <yichen.tang at rte-france.com>
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class CassandraAppStorageChecks {

    public static final String REF_NOT_FOUND = "REFERENCE_NOT_FOUND";
    public static final String ORPHAN_NODE = "ORPHAN_NODE";
    public static final String ORPHAN_DATA = "ORPHAN_DATA";
    public static final String INVALID_NODES = "INVALID_NODES";
    public static final Set<String> CHECK_NAMES = ImmutableSet.of(FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES,
        INVALID_NODES, REF_NOT_FOUND, ORPHAN_NODE, ORPHAN_DATA);

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAppStorageChecks.class);

    private final CassandraAppStorage storage;
    private final Supplier<Session> sessionSupplier;

    CassandraAppStorageChecks(CassandraAppStorage storage, Session session) {
        this(storage, () -> Objects.requireNonNull(session));
    }

    CassandraAppStorageChecks(CassandraAppStorage storage, Supplier<Session> sessionSupplier) {
        this.storage = Objects.requireNonNull(storage);
        this.sessionSupplier = Objects.requireNonNull(sessionSupplier);
    }

    private Session getSession() {
        return sessionSupplier.get();
    }

    List<String> getCheckNames() {
        return ImmutableList.copyOf(CHECK_NAMES);
    }

    List<FileSystemCheckIssue> check(FileSystemCheckOptions options) {
        List<FileSystemCheckIssue> results = new ArrayList<>();

        for (String type : options.getTypes()) {
            switch (type) {
                case FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES:
                    options.getInconsistentNodesExpirationTime()
                        .ifPresent(time -> checkInconsistent(results, time, options.isRepair()));
                    break;
                case INVALID_NODES:
                    results.addAll(checkInvalidNodes(options.isRepair()));
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
        ResultSet existingNodes = getSession().execute(select(ID).distinct().from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : existingNodes) {
            existingNodeIds.add(row.getUUID(ID));
        }
        ResultSet nodeDatas = getSession().execute(select(ID, NAME).distinct().from(NODE_DATA));
        for (Row row : nodeDatas) {
            UUID uuid = row.getUUID(ID);
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
            List<Statement> statements = new ArrayList<>();
            for (UUID id : orphanDataIds) {
                storage.removeAllData(id, statements);
            }
            executeStatements(statements);
        }
    }

    private void checkOrphanNode(List<FileSystemCheckIssue> results, FileSystemCheckOptions options) {
        //We may have:
        // nodes with "parent ID" which does not exist
        // nodes with null parent ID and which is not root ?
        // children nodes with "invalid" parent data :

        //Invalid data : ID which does not exist, ID null

        // get all child id which parent name is null
        ResultSet resultSet = getSession().execute(select(ID, CHILD_ID, NAME, CHILD_NAME).from(CHILDREN_BY_NAME_AND_CLASS));
        List<UUID> orphanIds = new ArrayList<>();
        Set<UUID> fakeParentIds = new HashSet<>();
        for (Row row : resultSet) {
            if (row.getString(NAME) == null) {
                UUID nodeId = row.getUUID(CHILD_ID);
                String nodeName = row.getString(CHILD_NAME);
                UUID fakeParentId = row.getUUID(ID);
                if (nodeId == null) {
                    //Invalid line in the table, we just delete it since we have no node ID to delete
                    FileSystemCheckIssue issue = new FileSystemCheckIssue().setNodeId(fakeParentId.toString())
                        .setNodeName(nodeName)
                        .setType(ORPHAN_NODE)
                        .setDescription("Invalid child named " + nodeName + " for node " + fakeParentId);

                    if (options.isRepair()) {
                        getSession().execute(delete()
                            .from(CHILDREN_BY_NAME_AND_CLASS)
                            .where(eq(ID, fakeParentId)).and(eq(CHILD_NAME, nodeName)));
                        issue.setRepaired(true);
                        issue.setResolutionDescription("Deleted invalid child " + nodeName);
                    }
                } else {
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
        }
        if (options.isRepair()) {
            orphanIds.forEach(storage::deleteNode);
            for (UUID fakeParentId : fakeParentIds) {
                getSession().execute(delete().from(CHILDREN_BY_NAME_AND_CLASS)
                    .where(eq(ID, fakeParentId)));
            }
        }
    }

    private void checkReferenceNotFound(List<FileSystemCheckIssue> results, FileSystemCheckOptions options) {
        List<Statement> statements = new ArrayList<>();
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
                statements.add(delete().from(CHILDREN_BY_NAME_AND_CLASS)
                    .where(eq(ID, childNodeInfo.parentId))
                    .and(eq(CHILD_NAME, childNodeInfo.name)));
                issue.setResolutionDescription("reset null child_name and child_id in " + childNodeInfo.parentId);
            }
        }
        if (options.isRepair()) {
            executeStatements(statements);
        }
    }

    private void executeStatements(List<Statement> statements) {
        for (Statement statement : statements) {
            getSession().execute(statement);
        }
    }

    private Set<ChildNodeInfo> getAllIdsInChildId() {
        Set<ChildNodeInfo> set = new HashSet<>();
        ResultSet resultSet = getSession().execute(select(CHILD_ID, CHILD_NAME, ID).from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : resultSet) {
            final UUID id = row.getUUID(CHILD_ID);
            if (id != null) {
                set.add(new ChildNodeInfo(id, row.getString(CHILD_NAME), row.getUUID(ID)));
            }
        }
        return set;
    }

    private Set<UUID> allPrimaryKeys() {
        Set<UUID> set = new HashSet<>();
        ResultSet resultSet = getSession().execute(select(ID).distinct().from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : resultSet) {
            set.add(row.getUUID(0));
        }
        return set;
    }

    private void checkInconsistent(List<FileSystemCheckIssue> results, Instant expirationTime, boolean repair) {
        ResultSet resultSet = getSession().execute(select(ID, NAME, MODIFICATION_DATE, CONSISTENT)
            .from(CHILDREN_BY_NAME_AND_CLASS));
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
        issue.setNodeId(row.getUUID(ID).toString()).setNodeName(row.getString(NAME));
        return issue;
    }

    private void repairExpirationInconsistent(FileSystemCheckIssue issue) {
        storage.deleteNode(issue.getNodeId());
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

    /**
     * Nodes with null name are considered invalid (name is mandatory in the data model).
     * TODO: we could also check for nullity of other require attributes, such as dates
     */
    private List<FileSystemCheckIssue> checkInvalidNodes(boolean repair) {
        List<FileSystemCheckIssue> issues = readAllNodes().stream()
            .filter(node -> node.getName() == null)
            .map(node -> new FileSystemCheckIssue()
                .setType(INVALID_NODES)
                .setNodeId(node.getId().toString())
                .setDescription("Node has no name, which is mandatory")
                .setResolutionDescription("Node will be deleted"))
            .collect(Collectors.toList());

        if (repair) {
            issues.forEach(issue -> {
                //cannot use storage.deleteNode
                getSession().execute(delete().from(CHILDREN_BY_NAME_AND_CLASS).where(eq(ID, UUID.fromString(issue.getNodeId()))));
                issue.setRepaired(true)
                     .setResolutionDescription("Node has been deleted");
            });
        }
        return issues;
    }

    /**
     * Reads the whole nodes table in memory
     */
    private List<NodeRow> readAllNodes() {
        List<NodeRow> nodes = new ArrayList<>();
        ResultSet res = getSession().execute(select(PARENT_ID, ID, NAME, CHILD_ID, CHILD_NAME).from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : res) {
            nodes.add(new NodeRow(row.getUUID(0), row.getUUID(1), row.getString(2), row.getUUID(3), row.getString(4)));
        }
        return nodes;
    }
}
