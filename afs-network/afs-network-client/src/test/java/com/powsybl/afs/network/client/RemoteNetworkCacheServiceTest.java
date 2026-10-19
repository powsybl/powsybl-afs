/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.network.client;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.ext.base.AbstractProjectCase;
import com.powsybl.afs.ext.base.ScriptType;
import com.powsybl.afs.ws.client.utils.ClientUtils;
import com.powsybl.afs.ws.client.utils.RemoteServiceConfig;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.Substation;
import com.powsybl.iidm.network.test.NetworkTest1Factory;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
@ExtendWith(MockitoExtension.class)
class RemoteNetworkCacheServiceTest {

    @Mock
    private Supplier<Optional<RemoteServiceConfig>> configSupplier;

    @Mock
    private AbstractProjectCase projectCase;

    @Mock
    private RemoteServiceConfig remoteServiceConfig;

    @Mock
    private Client client;

    @Mock
    private WebTarget webTarget;

    @Mock
    private AppFileSystem appFileSystem;

    @Mock
    Invocation.Builder builder;

    private FileSystem fileSystem;
    private RemoteNetworkCacheService networkCacheService;
    private Network network;
    private Path networkPath;

    @BeforeEach
    void setUp() throws IOException {
        network = NetworkTest1Factory.create();
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        Path testDir = fileSystem.getPath("/tmp");
        Files.createDirectories(testDir);
        networkPath = testDir.resolve("network.xiidm");
        network.write("XIIDM", new Properties(), networkPath);
        networkCacheService = new RemoteNetworkCacheService(configSupplier, "test-token");
    }

    @AfterEach
    void tearDown() throws IOException {
        fileSystem.close();
    }

    @Test
    void getNetworkShouldThrowWhenConfigMissing() {
        when(configSupplier.get()).thenReturn(Optional.empty());
        when(projectCase.getFileSystem()).thenReturn(appFileSystem);
        when(appFileSystem.getName()).thenReturn("test-app");

        AfsException exception = assertThrows(AfsException.class, () -> networkCacheService.getNetwork(projectCase));
        assertEquals("Remote service config is missing", exception.getMessage());
    }

    @Test
    void getNetworkTest() throws IOException {
        // Configuration
        when(configSupplier.get()).thenReturn(Optional.of(remoteServiceConfig));
        when(remoteServiceConfig.getRestUri()).thenReturn(URI.create("http://localhost:8080"));
        when(projectCase.getFileSystem()).thenReturn(appFileSystem);
        when(appFileSystem.getName()).thenReturn("test-app");
        when(projectCase.getId()).thenReturn("node-123");

        // Call the tested method
        Network result;
        try (MockedStatic<ClientUtils> mockedClientUtils = mockStatic(ClientUtils.class, Mockito.CALLS_REAL_METHODS);
             InputStream inputStream = Files.newInputStream(networkPath);
             Response response = Response.ok(inputStream).build()) {
            mockedClientUtils.when(ClientUtils::createClient).thenReturn(client);
            when(client.target(any(URI.class))).thenReturn(webTarget);
            when(webTarget.path(anyString())).thenReturn(webTarget);
            when(webTarget.resolveTemplate(anyString(), any())).thenReturn(webTarget);
            when(webTarget.request(MediaType.APPLICATION_XML)).thenReturn(builder);
            when(builder.header(anyString(), anyString())).thenReturn(builder);
            when(builder.get()).thenReturn(response);

            result = networkCacheService.getNetwork(projectCase);
        }

        // Checks
        assertNotNull(result);
        assertEquals(network.getId(), result.getId());
        network.getVoltageLevels().forEach(voltageLevel -> assertNotNull(result.getVoltageLevel(voltageLevel.getId())));
        assertEquals(StringUtils.EMPTY, networkCacheService.getOutput(projectCase));
    }

    @Test
    void queryNetworkTest() {
        // Configuration
        when(configSupplier.get()).thenReturn(Optional.of(remoteServiceConfig));
        when(remoteServiceConfig.getRestUri()).thenReturn(URI.create("http://localhost:8080"));
        when(projectCase.getFileSystem()).thenReturn(appFileSystem);
        when(appFileSystem.getName()).thenReturn("test-app");
        when(projectCase.getId()).thenReturn("node-123");

        // Call the tested method
        String result;
        try (MockedStatic<ClientUtils> mockedClientUtils = mockStatic(ClientUtils.class, Mockito.CALLS_REAL_METHODS);
             Response response = Response.ok(network.getSubstationStream().map(Substation::getId).toList().toString()).build()) {
            mockedClientUtils.when(ClientUtils::createClient).thenReturn(client);
            when(client.target(any(URI.class))).thenReturn(webTarget);
            when(webTarget.path(anyString())).thenReturn(webTarget);
            when(webTarget.resolveTemplate(anyString(), any())).thenReturn(webTarget);
            when(webTarget.queryParam(anyString(), any())).thenReturn(webTarget);
            when(webTarget.request(MediaType.TEXT_PLAIN)).thenReturn(builder);
            when(builder.header(anyString(), anyString())).thenReturn(builder);
            when(builder.post(any())).thenReturn(response);

            result = networkCacheService.queryNetwork(projectCase, ScriptType.GROOVY, "network.substations.collect { it.id }", Collections.emptyList(), Collections.emptyMap());
        }

        // Checks
        assertNotNull(result);
        assertEquals("[substation1]", result);
    }

    @Test
    void invalidateCacheTest() {
        // Configuration
        when(configSupplier.get()).thenReturn(Optional.of(remoteServiceConfig));
        when(remoteServiceConfig.getRestUri()).thenReturn(URI.create("http://localhost:8080"));
        when(projectCase.getFileSystem()).thenReturn(appFileSystem);
        when(appFileSystem.getName()).thenReturn("test-app");
        when(projectCase.getId()).thenReturn("node-123");

        // Call the tested method
        try (MockedStatic<ClientUtils> mockedClientUtils = mockStatic(ClientUtils.class, Mockito.CALLS_REAL_METHODS);
             Response response = Response.ok().build()) {
            mockedClientUtils.when(ClientUtils::createClient).thenReturn(client);
            when(client.target(any(URI.class))).thenReturn(webTarget);
            when(webTarget.path(anyString())).thenReturn(webTarget);
            when(webTarget.resolveTemplate(anyString(), any())).thenReturn(webTarget);
            when(webTarget.request()).thenReturn(builder);
            when(builder.header(anyString(), anyString())).thenReturn(builder);
            when(builder.delete()).thenReturn(response);

            assertDoesNotThrow(() -> networkCacheService.invalidateCache(projectCase));
        }
    }
}
