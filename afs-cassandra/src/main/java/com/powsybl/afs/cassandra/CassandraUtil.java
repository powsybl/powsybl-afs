/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

import java.io.PrintStream;
import java.util.Collection;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public final class CassandraUtil {

    private CassandraUtil() {
    }

    public static void print(CassandraContext context, PrintStream out) {
        Collection<TableMetadata> tables = context.getCluster().getMetadata()
                .getKeyspace(CassandraConstants.AFS_KEYSPACE)
                .getTables();
        tables.forEach(metadata -> {
            ResultSet resultSet = context.getSession().execute(select().json().all().from(metadata.getName()));
            boolean first = true;
            for (Row row : resultSet) {
                if (first) {
                    out.println(metadata.getName() + ":");
                    first = false;
                }
                String json = row.getString(0);
                out.println(json);
            }
        });
    }
}
