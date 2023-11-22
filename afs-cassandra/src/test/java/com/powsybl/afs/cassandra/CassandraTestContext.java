/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.cassandraunit.CassandraCQLUnit;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class CassandraTestContext implements CassandraContext {

    private final CassandraCQLUnit cassandraCQLUnit;

    public CassandraTestContext(CassandraCQLUnit cassandraCQLUnit) {
        this.cassandraCQLUnit = cassandraCQLUnit;
    }

    @Override
    public CqlSession getSession() {
        return cassandraCQLUnit.getSession();
    }

    @Override
    public boolean isClosed() {
        return cassandraCQLUnit.getSession().isClosed();
    }

    @Override
    public void close() {
    }
}
