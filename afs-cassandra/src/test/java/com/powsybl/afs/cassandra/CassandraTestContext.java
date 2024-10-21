/*
 * Copyright (c) 2019-2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraTestContext implements CassandraContext {

    private final CqlSession cassandraSession;

    public CassandraTestContext(CqlSession cassandraSession) {
        this.cassandraSession = cassandraSession;
    }

    @Override
    public CqlSession getSession() {
        return cassandraSession;
    }

    @Override
    public boolean isClosed() {
        return cassandraSession.isClosed();
    }

    @Override
    public void close() {
        // Nothing to do here
    }
}
