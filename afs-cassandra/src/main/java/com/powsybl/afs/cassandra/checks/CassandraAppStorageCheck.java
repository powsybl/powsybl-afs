/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra.checks;

import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;

import java.util.List;

/**
 * A sanity check for cassandra app storage.
 *
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public interface CassandraAppStorageCheck {

    /**
     * Name of this check
     */
    String getName();

    /**
     * Perform the app storage check and return identified issues.
     */
    List<FileSystemCheckIssue> check(CassandraAppStorageCheckSupport context, FileSystemCheckOptions options);

}
