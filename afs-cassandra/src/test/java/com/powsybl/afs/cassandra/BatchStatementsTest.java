package com.powsybl.afs.cassandra;

import java.util.HashSet;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.powsybl.afs.cassandra.CassandraAppStorage.BatchStatements;

public class BatchStatementsTest {

	
	@Test
	public void statements_are_added_to_builder() {
		// GIVEN
		BatchStatements statements = new BatchStatements(() -> new BatchStatementBuilder(BatchType.LOGGED),
				builder -> {});
		
		// WHEN
		for (int i = 0; i < 10; i++) {
			statements.addStatement(QueryBuilder.insertInto("NONE").value("COLONNE", QueryBuilder.literal("")).build());
		}
		
		// THEN
		Assertions.assertThat(statements.batchStatementBuilder.getStatementsCount()).isEqualTo(10);
	}
	
	@Test
	public void statements_are_executed_by_batch() {
		// GIVEN
		Set<BatchStatementBuilder> executions = new HashSet<>();
		BatchStatements statements = new BatchStatements(() -> new BatchStatementBuilder(BatchType.LOGGED),
				executions::add, 7);
		
		// WHEN
		for (int i = 0; i < 10; i++) {
			statements.addStatement(QueryBuilder.insertInto("NONE").value("COLONNE", QueryBuilder.literal("")).build());
		}
		
		// THEN
		Assertions.assertThat(executions.size()).isEqualTo(1);
		Assertions.assertThat(statements.batchStatementBuilder.getStatementsCount()).isLessThan(10);
		Assertions.assertThat(statements.batchStatementBuilder.getStatementsCount()).isGreaterThan(1);
	}
	
	@Test
	public void statements_are_executed_manually() {
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
		Assertions.assertThat(executions.size()).isEqualTo(1);
		Assertions.assertThat(statements.batchStatementBuilder.getStatementsCount()).isZero();
	}
	
	
}
