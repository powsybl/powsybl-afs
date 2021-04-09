/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.check;

import java.time.Instant;
import java.util.Objects;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class FileSystemCheckOptionsBuilder {

    private Instant inconsistentNodesExpirationTime = Instant.MIN;
    private boolean repair = false;

    public FileSystemCheckOptionsBuilder() {

    }

    public FileSystemCheckOptionsBuilder dryRun() {
        repair = false;
        return this;
    }

    public FileSystemCheckOptionsBuilder repair() {
        repair = true;
        return this;
    }

    public FileSystemCheckOptionsBuilder setInconsistentNodesExpirationTime(Instant inconsistentNodesExpirationTime) {
        this.inconsistentNodesExpirationTime = Objects.requireNonNull(inconsistentNodesExpirationTime);
        return this;
    }

    public FileSystemCheckOptions build() {
        return new FileSystemCheckOptions(inconsistentNodesExpirationTime, repair);
    }
}
