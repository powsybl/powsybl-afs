/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.powsybl.afs.storage.NodeGenericMetadata;

import java.util.Map;
import java.util.function.Function;

/**
 * @author Quentin CAPY <cappy.quentin at rte-france.com>
 */
enum NodeMetadataGetter {
    DOUBLE("md", NodeGenericMetadata::getDoubles),
    STRING("mt", NodeGenericMetadata::getStrings),
    INT("mi", NodeGenericMetadata::getInts),
    BOOLEAN("mb", NodeGenericMetadata::getBooleans);

    private String metaName;
    final Function<NodeGenericMetadata, Map<String, ?>> sup;

    NodeMetadataGetter(String metaName, Function<NodeGenericMetadata, Map<String, ?>> sup) {
        this.metaName = metaName;
        this.sup = sup;
    }

    Map<String, ?> apply(NodeGenericMetadata m) {
        return sup.apply(m);
    }

    String symbol() {
        return metaName;
    }

    String childSymbol() {
        return "c" + metaName;
    }

}
