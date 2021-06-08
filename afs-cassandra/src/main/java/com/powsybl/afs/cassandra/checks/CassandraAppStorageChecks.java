package com.powsybl.afs.cassandra.checks;

import com.google.common.collect.ImmutableList;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Diagnosis and repair tools for maintenance purpose.
 *
 * @author Yichen TANG <yichen.tang at rte-france.com>
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class CassandraAppStorageChecks {

    public static final List<CassandraAppStorageCheck> CHECKS = ImmutableList.of(
        new ReferenceNotFoundCheck(),
        new InvalidNodeCheck(),
        new OrphanNodeCheck(),
        new OrphanDataCheck(),
        new ExpiredInconsistentNodesCheck()
    );

    public List<String> getCheckNames() {
        return CHECKS.stream().map(CassandraAppStorageCheck::getName).collect(Collectors.toList());
    }

    public List<FileSystemCheckIssue> check(CassandraAppStorageCheckSupport support, FileSystemCheckOptions options) {
        return CHECKS.stream()
            .filter(check -> options.getTypes().contains(check.getName()))
            .flatMap(check -> check.check(support, options).stream())
            .collect(Collectors.toList());
    }
}
