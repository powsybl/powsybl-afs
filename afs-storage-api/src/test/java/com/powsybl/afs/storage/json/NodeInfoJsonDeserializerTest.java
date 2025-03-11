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
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class NodeInfoJsonDeserializerTest {

    private final NodeInfoJsonDeserializer deserializer = new NodeInfoJsonDeserializer();

    @Test
    void fullDeserializationTest() throws IOException {
        String json = """
            {
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
            }""";

        JsonParser parser = new JsonFactory().createParser(json);
        NodeInfo nodeInfo = deserializer.deserialize(parser, null);

        assertEquals("node-123", nodeInfo.getId());
        assertEquals("Test Node", nodeInfo.getName());
        assertEquals("CustomNode", nodeInfo.getPseudoClass());
        assertEquals("Description test", nodeInfo.getDescription());
        assertEquals(1672531200000L, nodeInfo.getCreationTime());
        assertEquals(1672617600000L, nodeInfo.getModificationTime());
        assertEquals(3, nodeInfo.getVersion());

        NodeGenericMetadata metadata = nodeInfo.getGenericMetadata();
        assertEquals("testValue", metadata.getString("nom"));
    }

    @Test
    void minimalDeserializationTest() throws IOException {
        String json = """
            {
                "id": "node-456",
                "name": "Minimal Node",
                "pseudoClass": "BasicNode",
                "description": "Description test",
                "metadata": [
                    {"type": "string", "name": "nom", "value": "testValue"}
                ]
            }""";

        JsonParser parser = new JsonFactory().createParser(json);
        NodeInfo nodeInfo = deserializer.deserialize(parser, null);

        assertEquals("node-456", nodeInfo.getId());
        assertEquals("Minimal Node", nodeInfo.getName());
        assertEquals("BasicNode", nodeInfo.getPseudoClass());
        assertEquals("Description test", nodeInfo.getDescription());
        assertEquals(-1, nodeInfo.getCreationTime());
        assertEquals(-1, nodeInfo.getModificationTime());
        assertEquals(-1, nodeInfo.getVersion());

        NodeGenericMetadata metadata = nodeInfo.getGenericMetadata();
        assertEquals("testValue", metadata.getString("nom"));
    }

    @Test
    void unexpectedFieldExceptionTest() throws IOException {
        String json = """
            {
                "id": "node-789",
                "invalidField": "badValue"
            }""";

        try (JsonParser parser = new JsonFactory().createParser(json)) {
            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> deserializer.deserialize(parser, null));
            assertEquals("Unexpected field: invalidField", exception.getMessage());
        }
    }

    @Test
    void missingRequiredFieldExceptionTest() throws IOException {
        String json = """
            {
                "name": "Invalid Node"
            }""";

        try (JsonParser parser = new JsonFactory().createParser(json)) {
            assertThrows(NullPointerException.class, () -> deserializer.deserialize(parser, null));
        }
    }
}
