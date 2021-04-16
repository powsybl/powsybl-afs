/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.check;

import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class FileSystemCheckOptionsBuilder {

    private Instant inconsistentNodesExpirationTime = Instant.MIN;
    private Set<String> ids = new HashSet<>();
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
     * For missing child node, it would clear reference record in its parent.
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
     * Parent's node id to check its children nodes status.
     * If child node is not exists, but still referenced under parent id. When retrieving, it will throw AfsException.
     * Check process would clear this referenced record.
     *
     * If child exists, do nothing.
     * @param ids
     * @return
     */
    public FileSystemCheckOptionsBuilder addParentNodeIds(Set<String> ids) {
        Objects.requireNonNull(ids);
        this.ids.addAll(ids);
        return this;
    }

    public FileSystemCheckOptions build() {
        return new FileSystemCheckOptions(inconsistentNodesExpirationTime, ids, repair);
    }
}
