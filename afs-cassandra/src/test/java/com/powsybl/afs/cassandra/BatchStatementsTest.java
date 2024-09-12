/*
 * Copyright (c) 2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.powsybl.afs.cassandra.CassandraAppStorage.BatchStatements;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

class BatchStatementsTest {

    @Test
    void statementsAreAddedToBuilder() {
        // GIVEN
        BatchStatements statements = new BatchStatements(() -> new BatchStatementBuilder(BatchType.LOGGED),
            builder -> { });

        // WHEN
        for (int i = 0; i < 10; i++) {
            statements.addStatement(QueryBuilder.insertInto("NONE").value("COLONNE", QueryBuilder.literal("")).build());
        }

        // THEN
        Assertions.assertThat(statements.getBatchStatementBuilder().getStatementsCount()).isEqualTo(10);
    }

    @Test
    void statementsAreExecutedByBatch() {
        // GIVEN
        Set<BatchStatementBuilder> executions = new HashSet<>();
        BatchStatements statements = new BatchStatements(() -> new BatchStatementBuilder(BatchType.LOGGED),
                executions::add, 7);

        // WHEN
        for (int i = 0; i < 10; i++) {
            statements.addStatement(QueryBuilder.insertInto("NONE").value("COLONNE", QueryBuilder.literal("")).build());
        }

        // THEN
        Assertions.assertThat(executions).hasSize(1);
        Assertions.assertThat(statements.getBatchStatementBuilder().getStatementsCount()).isLessThan(10);
        Assertions.assertThat(statements.getBatchStatementBuilder().getStatementsCount()).isGreaterThan(1);
    }

    @Test
    void statementsAreExecutedManually() {
        // GIVEN
        Set<BatchStatementBuilder> executions = new HashSet<>();
        BatchStatements statements = new BatchStatements(() -> new BatchStatementBuilder(BatchType.LOGGED),
                executions::add, 20);

        // WHEN
        for (int i = 0; i < 10; i++) {
            statements.addStatement(QueryBuilder.insertInto("NONE").value("COLONNE", QueryBuilder.literal("")).build());
        }
        statements.execute();

        // THEN
        Assertions.assertThat(executions).hasSize(1);
        Assertions.assertThat(statements.getBatchStatementBuilder().getStatementsCount()).isZero();
    }

}
