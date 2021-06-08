package com.powsybl.afs.cassandra.checks;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.powsybl.afs.cassandra.CassandraAppStorage;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;

import java.time.Instant;
import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.powsybl.afs.cassandra.CassandraConstants.*;

/**
 * Identifies and deletes inconsistent nodes which are older than a provided date.
 *
 * @author Yichen TANG <yichen.tang at rte-france.com>
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class ExpiredInconsistentNodesCheck implements CassandraAppStorageCheck {

    @Override
    public String getName() {
        return FileSystemCheckOptions.EXPIRED_INCONSISTENT_NODES;
    }

    @Override
    public List<FileSystemCheckIssue> check(CassandraAppStorageCheckSupport context, FileSystemCheckOptions options) {
        List<FileSystemCheckIssue> issues = new ArrayList<>();
        ResultSet resultSet = context.getSession().execute(select(ID, NAME, MODIFICATION_DATE, CONSISTENT)
            .from(CHILDREN_BY_NAME_AND_CLASS));
        Instant expiresBefore = options.getInconsistentNodesExpirationTime().orElse(null);
        if (expiresBefore == null) {
            return Collections.emptyList();
        }
        for (Row row : resultSet) {
            final Optional<FileSystemCheckIssue> issue = buildExpirationInconsistentIssue(row, expiresBefore);
            issue.ifPresent(issues::add);
        }
        if (options.isRepair()) {
            for (FileSystemCheckIssue issue : issues) {
                if (Objects.equals(issue.getType(), "inconsistent")) {
                    repairExpirationInconsistent(context.getStorage(), issue);
                }
            }
        }
        return issues;
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

    private void repairExpirationInconsistent(CassandraAppStorage storage, FileSystemCheckIssue issue) {
        storage.deleteNode(issue.getNodeId());
        issue.setRepaired(true);
        issue.setResolutionDescription("deleted");
    }
}
