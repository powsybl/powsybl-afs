/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.check;

import java.util.Objects;
import java.util.UUID;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class FileSystemCheckIssue {

    private UUID uuid;
    private String name;
    private Type type;
    private boolean repaired;

    public UUID getUuid() {
        return uuid;
    }

    public FileSystemCheckIssue setUuid(UUID uuid) {
        this.uuid = Objects.requireNonNull(uuid);
        return this;
    }

    public String getName() {
        return name;
    }

    public FileSystemCheckIssue setName(String name) {
        this.name = Objects.requireNonNull(name);
        return this;
    }

    public Type getType() {
        return type;
    }

    public FileSystemCheckIssue setType(Type type) {
        this.type = Objects.requireNonNull(type);
        return this;
    }

    public boolean isRepaired() {
        return repaired;
    }

    public FileSystemCheckIssue setRepaired(boolean repaired) {
        this.repaired = repaired;
        return this;
    }

    @Override
    public String toString() {
        return "FileSystemCheckIssue{" +
                "uuid=" + uuid +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", repaired=" + repaired +
                '}';
    }

    public enum Type {
        EXPIRATION_INCONSISTENT,
        MISSING_CHILD_NODE;
    }
}
