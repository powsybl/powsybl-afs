/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.storage.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.storage.NodeDependency;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class NodeDependencyDeserializerTest {
    private final NodeDependencyDeserializer deserializer = new NodeDependencyDeserializer();

    @Test
    void deserializeValidJsonTest() throws IOException {
        // Correct Json
        String json = """
            {
                "name": "testNode",
                "nodeInfo": {
                    "id": "node-123",
                    "name": "Test Node",
                    "pseudoClass": "CustomNode",
                    "description": "Description test",
                    "creationTime": 1672531200000,
                    "modificationTime": 1672617600000,
                    "version": 3,
                    "metadata": [
                        {"type": "string", "name": "nom", "value": "testValue"}
                    ]
                }
            }""";

        JsonParser jsonParser = new JsonFactory().createParser(json);
        NodeDependency dependency = deserializer.deserialize(jsonParser, null);

        // Checks
        assertNotNull(dependency);
        assertEquals("testNode", dependency.getName());
        assertEquals("node-123", dependency.getNodeInfo().getId());
    }

    @Test
    void deserializeMissingNameExceptionTest() throws Exception {
        // Json with missing name field
        String json = """
            {
                "nodeInfo": {
                    "id": "node-123",
                    "name": "Test Node",
                    "pseudoClass": "CustomNode",
                    "description": "Description test",
                    "creationTime": 1672531200000,
                    "modificationTime": 1672617600000,
                    "version": 3,
                    "metadata": [
                        {"type": "string", "name": "nom", "value": "testValue"}
                    ]
                }
            }""";

        try (JsonParser parser = new JsonFactory().createParser(json)) {
            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> deserializer.deserialize(parser, null));
            assertEquals("Inconsistent node dependency json", exception.getMessage());
        }
    }

    @Test
    void deserializeMissingNodeInfoExceptionTest() throws Exception {
        // Json with missing nodeInfo field
        String json = """
            {
                "name": "testNode"
            }""";

        try (JsonParser parser = new JsonFactory().createParser(json)) {
            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> deserializer.deserialize(parser, null));
            assertEquals("Inconsistent node dependency json", exception.getMessage());
        }
    }

    @Test
    void testDeserializeUnexpectedField() throws Exception {
        // Json with unexpected field
        String json = """
            {
                "name": "testNode",
                "unexpectedField": "value"
            }""";

        try (JsonParser parser = new JsonFactory().createParser(json)) {
            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> deserializer.deserialize(parser, null));
            assertEquals("Unexpected field: unexpectedField", exception.getMessage());
        }
    }
}
