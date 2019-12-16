/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

import java.util.List;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class CassandraSimpleContext implements CassandraContext {

    private final Cluster cluster;
    private final Session session;

    public CassandraSimpleContext(List<String> ipAddresses, String localDc) {
        // load balancing policy
        DCAwareRoundRobinPolicy.Builder loadBalancingPolicyBuilder = DCAwareRoundRobinPolicy.builder()
                .withUsedHostsPerRemoteDc(1)
                .allowRemoteDCsForLocalConsistencyLevel();
        if (localDc != null) {
            loadBalancingPolicyBuilder.withLocalDc(localDc);
        }
        DCAwareRoundRobinPolicy loadBalancingPolicy = loadBalancingPolicyBuilder.build();

        // query consistency
        QueryOptions queryOptions = new QueryOptions()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);

        // build cluster
        cluster = Cluster.builder()
                .addContactPoints(ipAddresses.toArray(new String[ipAddresses.size()]))
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withQueryOptions(queryOptions)
                .build();
        session = cluster.connect(CassandraConstants.AFS_KEYSPACE);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Session getSession() {
        return session;
    }

    @Override
    public boolean isClosed() {
        return cluster.isClosed();
    }

    @Override
    public void close() {
        cluster.close();
    }
}
