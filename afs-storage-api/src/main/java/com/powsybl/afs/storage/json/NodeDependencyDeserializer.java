/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.storage.NodeDependency;
import com.powsybl.afs.storage.NodeInfo;

import java.io.IOException;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class NodeDependencyDeserializer extends StdDeserializer<NodeDependency> {

    public NodeDependencyDeserializer() {
        super(NodeDependency.class);
    }

    @Override
    public NodeDependency deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        String name = null;
        NodeInfo nodeInfo = null;
        JsonToken token;
        while ((token = jsonParser.nextToken()) != null) {
            if (token == JsonToken.END_OBJECT) {
                break;
            } else if (token == JsonToken.FIELD_NAME) {
                switch (jsonParser.currentName()) {
                    case "name" -> {
                        jsonParser.nextToken();
                        name = jsonParser.getValueAsString();
                    }
                    case "nodeInfo" -> nodeInfo = new NodeInfoJsonDeserializer().deserialize(jsonParser, deserializationContext);
                    default -> throw new AfsStorageException("Unexpected field: " + jsonParser.currentName());

                }
            }
        }
        if (name == null || nodeInfo == null) {
            throw new AfsStorageException("Inconsistent node dependency json");
        }
        return new NodeDependency(name, nodeInfo);
    }
}
