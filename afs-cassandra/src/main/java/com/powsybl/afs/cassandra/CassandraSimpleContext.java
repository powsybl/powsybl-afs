/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraSimpleContext implements CassandraContext {

    private static final int DEFAULT_PORT = 9042;
    private final CqlSession session;

    public CassandraSimpleContext(List<String> ipAddresses, String localDc) {
        // init address with default port
        List<InetSocketAddress> inetSocketAddresses = ipAddresses.stream().map(ip -> new InetSocketAddress(ip, DEFAULT_PORT)).collect(Collectors.toList());
        // build Session
        CqlSessionBuilder cqlSessionBuilder = new CqlSessionBuilder()
                .addContactPoints(inetSocketAddresses);
        if (localDc != null) {
            cqlSessionBuilder.withLocalDatacenter(localDc);
        }
        cqlSessionBuilder.withKeyspace(CassandraConstants.AFS_KEYSPACE);
        session = cqlSessionBuilder.build();
    }

    public CqlSession getSession() {
        return session;
    }

    @Override
    public boolean isClosed() {
        return session.isClosed();
    }

    @Override
    public void close() {
        session.close();
    }
}
