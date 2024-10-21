/*
 * Copyright (c) 2024, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.cassandra;

import java.util.Objects;
import java.util.UUID;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
public class ChildNodeInfo {
    final UUID id;
    final String name;
    final UUID parentId;

    ChildNodeInfo(UUID id, String name, UUID parentId) {
        this.id = id;
        this.name = name;
        this.parentId = parentId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChildNodeInfo that)) {
            return false;
        }

        return id.equals(that.id);
    }
}
