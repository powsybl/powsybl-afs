/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import java.io.PrintStream;
import java.util.Map;
import java.util.Optional;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public final class CassandraUtil {

    private CassandraUtil() {
    }

    public static void print(CassandraContext context, PrintStream out) {

        Optional<KeyspaceMetadata> keyspace = context.getSession().getMetadata()
                .getKeyspace(CassandraConstants.AFS_KEYSPACE);
        if (!keyspace.isPresent()) {
            return;
        }

        Map<CqlIdentifier, TableMetadata> tables = keyspace.get().getTables();
        tables.keySet().forEach(metadata -> {
            String tableName = tables.get(metadata).getName().toString();
            ResultSet resultSet = context.getSession().execute(selectFrom(tableName).all().build());
            boolean first = true;
            for (Row row : resultSet) {
                if (first) {
                    out.println(tableName + ":");
                    first = false;
                }
                String json = row.getString(0);
                out.println(json);
            }
        });
    }
}
