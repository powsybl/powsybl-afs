/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
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
