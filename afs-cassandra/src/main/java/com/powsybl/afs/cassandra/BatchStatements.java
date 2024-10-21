/*
 * Copyright (c) 2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;

import java.util.function.Supplier;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
public class BatchStatements {
    private static final int DEFAULT_COUNT_TRESHOLD = 50_000;

    private final int countTreshold;
    private final Supplier<BatchStatementBuilder> supplier;
    private final MessagePassingQueue.Consumer<BatchStatementBuilder> consumer;
    BatchStatementBuilder batchStatementBuilder;

    public BatchStatements(Supplier<BatchStatementBuilder> supplier, MessagePassingQueue.Consumer<BatchStatementBuilder> consumer, int threshold) {
        this.countTreshold = threshold;
        this.supplier = supplier;
        this.consumer = consumer;
        this.batchStatementBuilder = supplier.get();
    }

    public BatchStatements(Supplier<BatchStatementBuilder> supplier, MessagePassingQueue.Consumer<BatchStatementBuilder> consumer) {
        this(supplier, consumer, DEFAULT_COUNT_TRESHOLD);
    }

    public void addStatement(SimpleStatement statement) {
        if (batchStatementBuilder.getStatementsCount() >= countTreshold) {
            execute();
        }
        batchStatementBuilder.addStatement(statement);
    }

    public void execute() {
        consumer.accept(batchStatementBuilder);
        batchStatementBuilder = supplier.get();
    }

    public BatchStatementBuilder getBatchStatementBuilder() {
        return batchStatementBuilder;
    }
}
