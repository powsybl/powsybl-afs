package com.powsybl.afs.cassandra.checks;

import com.datastax.driver.core.Session;
import com.powsybl.afs.cassandra.CassandraAppStorage;

import java.util.UUID;

/**
 * Provides some support for checks, like access to cassandra session,
 * or access to internal methods of the app storage. To be provided
 * by the app storage.
 *
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public interface CassandraAppStorageCheckSupport {

    CassandraAppStorage getStorage();

    Session getSession();

    void removeData(UUID nodeId);
}
