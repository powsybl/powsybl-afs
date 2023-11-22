/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.check;

import java.util.Objects;

/**
 * An issue identified during a file system check.
 *
 * @author Yichen TANG {@literal <yichen.tang at rte-france.com>}
 */
public class FileSystemCheckIssue {

    private String nodeId;
    private String nodeName;
    private String type;

    private String description;
    private String resolutionDescription;
    private boolean repaired;

    /**
     * The id of the node on which the issue was identified
     */
    public String getNodeId() {
        return nodeId;
    }

    public FileSystemCheckIssue setNodeId(String nodeId) {
        this.nodeId = Objects.requireNonNull(nodeId);
        return this;
    }

    /**
     * The name of the node on which the issue was identified
     */
    public String getNodeName() {
        return nodeName;
    }

    public FileSystemCheckIssue setNodeName(String nodeName) {
        this.nodeName = Objects.requireNonNull(nodeName);
        return this;
    }

    /**
     * The issue type.
     */
    public String getType() {
        return type;
    }

    public FileSystemCheckIssue setType(String type) {
        this.type = Objects.requireNonNull(type);
        return this;
    }

    /**
     * {@code true} if the issue has been repaired. If so, a message will describe what has been done.
     * @see #getResolutionDescription()
     */
    public boolean isRepaired() {
        return repaired;
    }

    public FileSystemCheckIssue setRepaired(boolean repaired) {
        this.repaired = repaired;
        return this;
    }

    /**
     * A message describing the identified issue.
     */
    public String getDescription() {
        return description;
    }

    public FileSystemCheckIssue setDescription(String description) {
        this.description = description;
        return this;
    }

    /**
     * A message describing what has been done to repair the issue (if repaired).
     */
    public String getResolutionDescription() {
        return resolutionDescription;
    }

    public FileSystemCheckIssue setResolutionDescription(String resolutionDescription) {
        this.resolutionDescription = resolutionDescription;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeName, type, description, resolutionDescription, repaired);
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
            Objects.equals(nodeName, that.nodeName) &&
            Objects.equals(type, that.type) &&
            Objects.equals(description, that.description) &&
            Objects.equals(resolutionDescription, that.resolutionDescription);
    }

    @Override
    public String toString() {
        return "FileSystemCheckIssue{" +
                "nodeId=" + nodeId +
                ", name='" + nodeName + '\'' +
                ", type=" + type +
                ", description=" + description +
                ", resolutionDescription=" + resolutionDescription +
                ", repaired=" + repaired +
                '}';
    }

}
