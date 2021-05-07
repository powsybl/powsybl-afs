/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.check;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class FileSystemCheckOptionsBuilder {

    private Instant inconsistentNodesExpirationTime;
    private Set<String> types = new HashSet<>();
    private boolean repair = false;

    public FileSystemCheckOptionsBuilder() {
    }

    /**
     * Just report issues, but not to fix.
     * @return
     */
    public FileSystemCheckOptionsBuilder dryRun() {
        repair = false;
        return this;
    }

    /**
     * For inconsistent expiration node, it would delete those nodes.
     * @return
     */
    public FileSystemCheckOptionsBuilder repair() {
        repair = true;
        return this;
    }

    public FileSystemCheckOptionsBuilder setInconsistentNodesExpirationTime(Instant inconsistentNodesExpirationTime) {
        this.inconsistentNodesExpirationTime = Objects.requireNonNull(inconsistentNodesExpirationTime);
        return this;
    }

    /**
     * Add implementation specific check types.
     * For example, in cassandra, "reference_not_found"
     *
     * @param types
     * @return
     */
    public FileSystemCheckOptionsBuilder addCheckTypes(Collection<String> types) {
        Objects.requireNonNull(types);
        this.types.addAll(types);
        return this;
    }

    public FileSystemCheckOptions build() {
        return new FileSystemCheckOptions(inconsistentNodesExpirationTime, types, repair);
    }
}
