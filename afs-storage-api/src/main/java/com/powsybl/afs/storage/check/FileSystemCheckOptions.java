/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.check;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 *
 * Options for a file system check. It defines what types of issues should be looked for,
 * and if they should be repaired or not.
 *
 * @author Yichen TANG {@literal <yichen.tang at rte-france.com>}
 */
public class FileSystemCheckOptions {

    /**
     * If defined in {@link #getTypes()}, the implementation must look for
     * inconsistent nodes older than a specified date.
     */
    public static final String EXPIRED_INCONSISTENT_NODES = "EXPIRED_INCONSISTENT_NODES";

    private final Instant inconsistentNodesExpirationTime; //Inconsistent nodes older than this could be deleted
    private final Set<String> types;
    private final boolean repair; //option for trying to solve all issues

    FileSystemCheckOptions(Instant expiration, Set<String> types, boolean repair) {
        inconsistentNodesExpirationTime = expiration;
        this.types = Collections.unmodifiableSet(types);
        this.repair = repair;
    }

    /**
     * Inconsistent nodes older than this are considered "expired", which means
     * that their creation will probably never be achieved. If {@link #isRepair()}
     * is {@code true}, those nodes will be removed.
     * If absent, nothing is done.
     */
    public Optional<Instant> getInconsistentNodesExpirationTime() {
        return Optional.ofNullable(inconsistentNodesExpirationTime);
    }

    /**
     * Defines the type of issues which should be looked for.
     * Types can be implementation-dependent.
     */
    public Set<String> getTypes() {
        return types;
    }

    /**
     * If {@code false}, just list issues but don't repair them
     */
    public boolean isRepair() {
        return repair;
    }
}
