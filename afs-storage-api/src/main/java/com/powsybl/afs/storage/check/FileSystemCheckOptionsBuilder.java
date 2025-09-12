/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.check;

import java.time.Instant;
import java.util.*;

/**
 * @author Yichen TANG {@literal <yichen.tang at rte-france.com>}
 */
public class FileSystemCheckOptionsBuilder {

    private Instant inconsistentNodesExpirationTime;
    private final Set<String> types = new HashSet<>();
    private boolean repair = false;

    public FileSystemCheckOptionsBuilder() {
        // Nothing here
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
     */
    public FileSystemCheckOptionsBuilder addCheckTypes(Collection<String> types) {
        Objects.requireNonNull(types);
        this.types.addAll(types);
        return this;
    }

    /**
     * Add implementation specific check types.
     */
    public FileSystemCheckOptionsBuilder addCheckTypes(String... types) {
        return addCheckTypes(Arrays.asList(types));
    }

    public FileSystemCheckOptions build() {
        return new FileSystemCheckOptions(inconsistentNodesExpirationTime, types, repair);
    }
}
