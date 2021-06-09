/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra.checks;

import com.google.common.collect.ImmutableList;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Diagnosis and repair tools for maintenance purpose.
 *
 * @author Yichen TANG <yichen.tang at rte-france.com>
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class CassandraAppStorageChecks {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAppStorageChecks.class);

    public static final List<CassandraAppStorageCheck> CHECKS = ImmutableList.of(
        new ReferenceNotFoundCheck(),
        new InvalidNodeCheck(),
        new OrphanNodeCheck(),
        new OrphanDataCheck(),
        new ExpiredInconsistentNodesCheck()
    );

    public static List<String> getCheckNames() {
        return CHECKS.stream().map(CassandraAppStorageCheck::getName).collect(Collectors.toList());
    }

    public List<FileSystemCheckIssue> check(CassandraAppStorageCheckSupport support, FileSystemCheckOptions options) {
        checkAndWarning(options);
        return CHECKS.stream()
            .filter(check -> options.getTypes().contains(check.getName()))
            .flatMap(check -> check.check(support, options).stream())
            .collect(Collectors.toList());
    }

    private static void checkAndWarning(FileSystemCheckOptions options) {
        Set<String> set = new HashSet<>(options.getTypes());
        set.removeAll(getCheckNames());
        if (!set.isEmpty()) {
            LOGGER.warn("Not supported check type:" + set);
        }
    }
}
