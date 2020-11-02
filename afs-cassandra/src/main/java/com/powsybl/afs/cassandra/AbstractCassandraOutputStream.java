/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.Session;

import java.io.OutputStream;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
abstract class AbstractCassandraOutputStream extends OutputStream {

    final UUID nodeUuid;

    final String name;

    final int chunkSize;

    final Session session;

    int chunkNum = 0;

    AbstractCassandraOutputStream(UUID nodeUuid, String name, int chunkSize, Session session) {
        this.nodeUuid = Objects.requireNonNull(nodeUuid);
        this.name = Objects.requireNonNull(name);
        this.chunkSize = chunkSize;
        this.session = Objects.requireNonNull(session);
    }
}
