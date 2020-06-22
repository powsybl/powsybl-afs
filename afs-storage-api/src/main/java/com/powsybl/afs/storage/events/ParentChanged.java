/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class ParentChanged extends NodeEvent {

    public static final String TYPENAME = "PARENT_CHANGED";

    @JsonProperty("oldParentId")
    private final String oldParentId;

    @JsonProperty("newParentId")
    private final String newParentId;

    @JsonCreator
    public ParentChanged(@JsonProperty("id") String id, @JsonProperty("oldParentId") String oldParentId, @JsonProperty("newParentId") String newParentId) {
        super(id, TYPENAME);
        Objects.requireNonNull(oldParentId);
        Objects.requireNonNull(newParentId);
        this.oldParentId = oldParentId;
        this.newParentId = newParentId;
    }

    public String getNewParentId() {
        return newParentId;
    }

    public String getOldParentId() {
        return oldParentId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParentChanged that = (ParentChanged) o;
        return Objects.equals(oldParentId, that.oldParentId) &&
                Objects.equals(newParentId, that.newParentId) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldParentId, newParentId, id);
    }

    @Override
    public String toString() {
        return "ParentChanged{" +
                "oldParentId='" + oldParentId + '\'' +
                ", newParentId='" + newParentId + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
