/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class StartTaskEvent extends TaskEvent {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("nodeId")
    private final String nodeId;

    @JsonCreator
    public StartTaskEvent(@JsonProperty("taskId") UUID taskId,
                          @JsonProperty("revision") long revision,
                          @JsonProperty("name") String name,
                          @JsonProperty("nodeId") String nodeId) {
        super(taskId, revision);
        this.name = Objects.requireNonNull(name);
        this.nodeId = nodeId;
    }

    public StartTaskEvent(@JsonProperty("taskId") UUID taskId, @JsonProperty("revision") long revision, @JsonProperty("name") String name) {
        this(taskId, revision, name, null);
    }

    public String getName() {
        return name;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, revision, name, nodeId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StartTaskEvent other) {
            if (nodeId != null && !nodeId.equals(other.nodeId)) {
                return false;
            }
            return taskId.equals(other.taskId) &&
                    revision == other.revision &&
                    name.equals(other.name);
        }
        return false;
    }

    @Override
    public String toString() {
        return "StartTaskEvent(taskId=" + taskId + ", revision=" + revision + ", nodeId=" + nodeId + ", name=" + name + ")";
    }
}
