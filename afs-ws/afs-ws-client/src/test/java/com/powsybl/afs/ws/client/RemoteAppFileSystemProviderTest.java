/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ws.client;

import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProviderContext;
import com.powsybl.afs.ws.client.utils.RemoteServiceConfig;
import com.powsybl.afs.ws.storage.RemoteAppStorage;
import com.powsybl.afs.ws.storage.RemoteTaskMonitor;
import jakarta.ws.rs.ProcessingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
@ExtendWith(MockitoExtension.class)
class RemoteAppFileSystemProviderTest {

    @Test
    void getFileSystemsNullContextExceptionTest() {
        RemoteAppFileSystemProvider provider = new RemoteAppFileSystemProvider(Optional::empty);
        assertThrows(NullPointerException.class, () -> provider.getFileSystems(null));
    }

    @Test
    void getFileSystemsWithMissingConfigTest() {
        RemoteAppFileSystemProvider provider = new RemoteAppFileSystemProvider(Optional::empty);
        AppFileSystemProviderContext context = mock(AppFileSystemProviderContext.class);
        List<AppFileSystem> fileSystems = provider.getFileSystems(context);
        assertTrue(fileSystems.isEmpty());
    }

    @Test
    void getFileSystemsWithProcessingExceptionTest() {
        // Mock some URI for the test
        RemoteServiceConfig config = mock(RemoteServiceConfig.class);
        when(config.getRestUri()).thenReturn(URI.create("http://test-uri"));

        // Mock a token
        AppFileSystemProviderContext context = mock(AppFileSystemProviderContext.class);
        when(context.getToken()).thenReturn("test-token");

        // Initialize the provider
        RemoteAppFileSystemProvider provider = new RemoteAppFileSystemProvider(() -> Optional.of(config));

        // Mock the exception on the RemoteAppStorage
        try (var mockedStatic = mockStatic(RemoteAppStorage.class)) {
            mockedStatic.when(() -> RemoteAppStorage.getFileSystemNames(any(), any()))
                .thenThrow(new ProcessingException("Test exception"));

            // Check that we get an empty list
            List<AppFileSystem> fileSystems = provider.getFileSystems(context);
            assertTrue(fileSystems.isEmpty());
        }
    }

    @Test
    void testGetFileSystemsWithValidConfig() {
        // Mock some URI for the test
        RemoteServiceConfig config = mock(RemoteServiceConfig.class);
        when(config.getRestUri()).thenReturn(URI.create("http://test-uri"));

        // Mock a token
        AppFileSystemProviderContext context = mock(AppFileSystemProviderContext.class);
        when(context.getToken()).thenReturn("test-token");

        // Initialize the provider with a spy in order to mock some methods inside
        RemoteAppFileSystemProvider provider = spy(new RemoteAppFileSystemProvider(() -> Optional.of(config)));

        try (RemoteAppStorage mockStorage = mock(RemoteAppStorage.class);
            RemoteTaskMonitor mockTaskMonitor = mock(RemoteTaskMonitor.class);
            var mockedStaticStorage = mockStatic(RemoteAppStorage.class)) {
            mockedStaticStorage.when(() -> RemoteAppStorage.getFileSystemNames(any(), any()))
                .thenReturn(Collections.singletonList("test-fs"));

            // Stub de la factory method
            doReturn(mockStorage).when(provider).createRemoteAppStorage(any(), any(), any(), any());
            doReturn(mockTaskMonitor).when(provider).createRemoteTaskMonitor(any(), any(), any());

            // Check that we get the correct list
            List<AppFileSystem> fileSystems = provider.getFileSystems(context);
            assertEquals(1, fileSystems.size());
            assertEquals("test-fs", fileSystems.get(0).getName());
        }
    }
}
