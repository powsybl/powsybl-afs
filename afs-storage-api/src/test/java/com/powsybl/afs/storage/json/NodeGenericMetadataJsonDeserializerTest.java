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
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class NodeGenericMetadataJsonDeserializerTest {

    private final NodeGenericMetadataJsonDeserializer deserializer = new NodeGenericMetadataJsonDeserializer();

    @Test
    void stringMetadataTest() throws IOException {
        String json = """
            [
                {"type": "string", "name": "nom", "value": "testValue"}
            ]""";

        JsonParser parser = new JsonFactory().createParser(json);
        NodeGenericMetadata metadata = deserializer.deserialize(parser, null);

        assertEquals("testValue", metadata.getString("nom"));
    }

    @Test
    void doubleMetadataTest() throws IOException {
        String json = """
            [
                {"type": "double", "name": "pi", "value": 3.14159}
            ]""";

        JsonParser parser = new JsonFactory().createParser(json);
        NodeGenericMetadata metadata = deserializer.deserialize(parser, null);

        assertEquals(3.14159, metadata.getDouble("pi"), 0.00001);
    }

    @Test
    void intMetadataTest() throws IOException {
        String json = """
            [
                {"type": "int", "name": "nombre", "value": 42}
            ]""";

        JsonParser parser = new JsonFactory().createParser(json);
        NodeGenericMetadata metadata = deserializer.deserialize(parser, null);

        assertEquals(42, metadata.getInt("nombre"));
    }

    @Test
    void booleanMetadataTest() throws IOException {
        String json = """
            [
                {"type": "boolean", "name": "actif", "value": true}
            ]""";

        JsonParser parser = new JsonFactory().createParser(json);
        NodeGenericMetadata metadata = deserializer.deserialize(parser, null);

        assertTrue(metadata.getBoolean("actif"));
    }

    @Test
    void multipleMetadataTest() throws IOException {
        String json = """
            [
                {"type": "string", "name": "nom", "value": "testValue"},
                {"type": "double", "name": "pi", "value": 3.14159},
                {"type": "int", "name": "nombre", "value": 42},
                {"type": "boolean", "name": "actif", "value": true}
            ]""";

        JsonParser parser = new JsonFactory().createParser(json);
        NodeGenericMetadata metadata = deserializer.deserialize(parser, null);

        assertEquals("testValue", metadata.getString("nom"));
        assertEquals(3.14159, metadata.getDouble("pi"), 0.00001);
        assertEquals(42, metadata.getInt("nombre"));
        assertTrue(metadata.getBoolean("actif"));
    }

    @Test
    void missingNameExceptionTest() throws IOException {
        String json = """
            [
                {"type": "string", "value": "ValeurSansNom"}
            ]""";

        try (JsonParser parser = new JsonFactory().createParser(json)) {
            assertThrows(NullPointerException.class, () -> deserializer.deserialize(parser, null));
        }
    }

    @Test
    void missingTypeExceptionTest() throws IOException {
        String json = """
            [
                {"name": "sansType", "value": "ValeurSansType"}
            ]""";

        try (JsonParser parser = new JsonFactory().createParser(json)) {
            assertThrows(NullPointerException.class, () -> deserializer.deserialize(parser, null));
        }
    }

    @Test
    void unexpectedMetadataTypeExceptionTest() throws IOException {
        String json = """
            [
                {"type": "invalid", "name": "invalide", "value": "invalidValue"}
            ]""";

        try (JsonParser parser = new JsonFactory().createParser(json)) {
            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> deserializer.deserialize(parser, null));
            assertEquals("Unexpected metadata type: invalid", exception.getMessage());
        }
    }

    @Test
    void unexpectedFieldExceptionTest() throws IOException {
        String json = """
            [
                {"type": "string", "name": "nom", "unexpected": "valeur"}
            ]""";

        try (JsonParser parser = new JsonFactory().createParser(json)) {
            AfsStorageException exception = assertThrows(AfsStorageException.class, () -> deserializer.deserialize(parser, null));
            assertEquals("Unexpected field: unexpected", exception.getMessage());
        }
    }
}
