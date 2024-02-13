/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class TimeSeriesCleared extends NodeEvent {

    public static final String TYPENAME = "TIME_SERIES_CLEARED";

    @JsonCreator
    public TimeSeriesCleared(@JsonProperty("id") String id) {
        super(id, TYPENAME);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TimeSeriesCleared other) {
            return id.equals(other.id);
        }
        return false;
    }

    @Override
    public String toString() {
        return "TimeSeriesCleared(id=" + id + ")";
    }
}
