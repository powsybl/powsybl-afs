/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.check;

import java.util.Objects;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class FileSystemCheckIssue {

    private String nodeId;
    private String name;
    private String type;

    private String description;
    private String resolutionDescription;
    private boolean repaired;

    public String getNodeId() {
        return nodeId;
    }

    public FileSystemCheckIssue setNodeId(String nodeId) {
        this.nodeId = Objects.requireNonNull(nodeId);
        return this;
    }

    public String getName() {
        return name;
    }

    public FileSystemCheckIssue setName(String name) {
        this.name = Objects.requireNonNull(name);
        return this;
    }

    public String getType() {
        return type;
    }

    public FileSystemCheckIssue setType(String type) {
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

    public String getDescription() {
        return description;
    }

    public FileSystemCheckIssue setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getResolutionDescription() {
        return resolutionDescription;
    }

    public FileSystemCheckIssue setResolutionDescription(String resolutionDescription) {
        this.resolutionDescription = resolutionDescription;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, name, type, description, resolutionDescription, repaired);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSystemCheckIssue that = (FileSystemCheckIssue) o;
        return repaired == that.repaired &&
            Objects.equals(nodeId, that.nodeId) &&
            Objects.equals(name, that.name) &&
            Objects.equals(type, that.type) &&
            Objects.equals(description, that.description) &&
            Objects.equals(resolutionDescription, that.resolutionDescription);
    }

    @Override
    public String toString() {
        return "FileSystemCheckIssue{" +
                "nodeId=" + nodeId +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", description=" + description +
                ", resolutionDescription=" + resolutionDescription +
                ", repaired=" + repaired +
                '}';
    }

}
