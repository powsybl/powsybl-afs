/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra.checks;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.powsybl.afs.cassandra.CassandraConstants;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;

import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.powsybl.afs.cassandra.CassandraConstants.*;

/**
 * Identifies and deletes AFS data linked to non existing node IDs.
 *
 * @author Yichen TANG <yichen.tang at rte-france.com>
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class OrphanDataCheck implements CassandraAppStorageCheck {

    public static final String NAME = "ORPHAN_DATA";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<FileSystemCheckIssue> check(CassandraAppStorageCheckSupport context, FileSystemCheckOptions options) {
        List<FileSystemCheckIssue> issues = new ArrayList<>();
        Set<UUID> existingNodeIds = new HashSet<>();
        Set<UUID> orphanDataIds = new HashSet<>();
        ResultSet existingNodes = context.getSession().execute(select(ID).distinct().from(CHILDREN_BY_NAME_AND_CLASS));
        for (Row row : existingNodes) {
            existingNodeIds.add(row.getUUID(ID));
        }
        ResultSet nodeDatas = context.getSession().execute(select(ID, CassandraConstants.NAME).distinct().from(NODE_DATA));
        for (Row row : nodeDatas) {
            UUID uuid = row.getUUID(ID);
            if (!existingNodeIds.contains(uuid)) {
                orphanDataIds.add(uuid);
                FileSystemCheckIssue issue = new FileSystemCheckIssue().setNodeName("N/A")
                    .setNodeId(uuid.toString())
                    .setType(getName())
                    .setDescription("Orphan data(" + row.getString(CassandraConstants.NAME) + ") is binding to non-existing node(" + uuid + ")")
                    .setRepaired(options.isRepair());
                if (options.isRepair()) {
                    issue.setRepaired(true)
                        .setResolutionDescription("Delete orphan data(" + row.getString(CassandraConstants.NAME) + ").");
                }
                issues.add(issue);
            }
        }
        if (options.isRepair()) {
            orphanDataIds.forEach(context::removeData);
        }
        return issues;
    }
}
