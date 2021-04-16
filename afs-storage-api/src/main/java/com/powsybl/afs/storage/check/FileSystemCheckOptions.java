/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.check;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class FileSystemCheckOptions {

    private final Instant inconsistentNodesExpirationTime; //Inconsistent nodes older than this could be deleted
    private final Set<String> ids;
    private final boolean repair; //option for trying to solve all issues

    FileSystemCheckOptions(Instant expiration, Set<String> ids, boolean repair) {
        inconsistentNodesExpirationTime = Objects.requireNonNull(expiration);
        this.ids = Collections.unmodifiableSet(ids);
        this.repair = repair;
    }

    /**
     * Use this time to compare with node's modification time.
     * @return
     */
    public Instant getInconsistentNodesExpirationTime() {
        return inconsistentNodesExpirationTime;
    }

    public Set<String> getIds() {
        return ids;
    }

    /**
     * @return If false, just list issues but not to repair.
     */
    public boolean isRepair() {
        return repair;
    }
}
