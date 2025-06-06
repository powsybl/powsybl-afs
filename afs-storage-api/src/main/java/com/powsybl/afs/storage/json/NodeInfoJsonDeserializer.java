/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
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
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;

import java.io.IOException;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class NodeInfoJsonDeserializer extends StdDeserializer<NodeInfo> {

    private static final class JsonParsingContext {
        String id = null;
        String name = null;
        String pseudoClass = null;
        String description = null;
        long creationTime = -1;
        long modificationTime = -1;
        int version = -1;
        NodeGenericMetadata metadata;
    }

    public NodeInfoJsonDeserializer() {
        super(NodeInfo.class);
    }

    private static void parseFieldName(JsonParser jsonParser, DeserializationContext deserializationContext,
                                       JsonParsingContext parsingContext) throws IOException {
        switch (jsonParser.currentName()) {
            case NodeInfoJsonSerializer.ID -> {
                jsonParser.nextToken();
                parsingContext.id = jsonParser.getValueAsString();
            }
            case NodeInfoJsonSerializer.NAME -> {
                jsonParser.nextToken();
                parsingContext.name = jsonParser.getValueAsString();
            }
            case NodeInfoJsonSerializer.PSEUDO_CLASS -> {
                jsonParser.nextToken();
                parsingContext.pseudoClass = jsonParser.getValueAsString();
            }
            case NodeInfoJsonSerializer.DESCRIPTION -> {
                jsonParser.nextToken();
                parsingContext.description = jsonParser.getValueAsString();
            }
            case NodeInfoJsonSerializer.CREATION_TIME -> {
                jsonParser.nextToken();
                parsingContext.creationTime = jsonParser.getValueAsLong();
            }
            case NodeInfoJsonSerializer.MODIFICATION_TIME -> {
                jsonParser.nextToken();
                parsingContext.modificationTime = jsonParser.getValueAsLong();
            }
            case NodeInfoJsonSerializer.VERSION -> {
                jsonParser.nextToken();
                parsingContext.version = jsonParser.getValueAsInt();
            }
            case NodeInfoJsonSerializer.METADATA -> {
                jsonParser.nextToken();
                parsingContext.metadata = new NodeGenericMetadataJsonDeserializer().deserialize(jsonParser, deserializationContext);
            }
            default -> throw new AfsStorageException("Unexpected field: " + jsonParser.currentName());
        }
    }

    @Override
    public NodeInfo deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        JsonParsingContext parsingContext = new JsonParsingContext();
        JsonToken token;
        while ((token = jsonParser.nextToken()) != null) {
            if (token == JsonToken.FIELD_NAME) {
                parseFieldName(jsonParser, deserializationContext, parsingContext);
            } else if (token == JsonToken.END_OBJECT) {
                break;
            }
        }
        return new NodeInfo(parsingContext.id, parsingContext.name, parsingContext.pseudoClass, parsingContext.description,
                            parsingContext.creationTime, parsingContext.modificationTime, parsingContext.version, parsingContext.metadata);
    }
}
