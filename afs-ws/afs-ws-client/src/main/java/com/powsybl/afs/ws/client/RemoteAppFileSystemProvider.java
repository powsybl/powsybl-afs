/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.client;

import com.google.auto.service.AutoService;
import com.google.common.base.Supplier;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProvider;
import com.powsybl.afs.AppFileSystemProviderContext;
import com.powsybl.afs.ws.client.utils.RemoteServiceConfig;
import com.powsybl.afs.ws.storage.RemoteAppStorage;
import com.powsybl.afs.ws.storage.RemoteTaskMonitor;
import com.powsybl.afs.ws.storage.websocket.WebsocketConnectionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.ProcessingException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Ali Tahanout {@literal <ali.tahanout at rte-france.com>}
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@AutoService(AppFileSystemProvider.class)
public class RemoteAppFileSystemProvider implements AppFileSystemProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteAppFileSystemProvider.class);

    private final Supplier<Optional<RemoteServiceConfig>> configSupplier;

    public RemoteAppFileSystemProvider() {
        this(RemoteServiceConfig::load);
    }

    public RemoteAppFileSystemProvider(Supplier<Optional<RemoteServiceConfig>> configSupplier) {
        this.configSupplier = Objects.requireNonNull(configSupplier);
    }

    @Override
    public List<AppFileSystem> getFileSystems(AppFileSystemProviderContext context) {
        Objects.requireNonNull(context);
        RemoteServiceConfig config = configSupplier.get().orElse(null);
        if (config != null) {
            URI uri = config.getRestUri();
            try {
                return RemoteAppStorage.getFileSystemNames(uri, context.getToken()).stream()
                        .map(fileSystemName -> {
                            WebsocketConnectionPolicy websocketPolicy = WebsocketConnectionPolicy.forConfig(config);
                            RemoteAppStorage storage = createRemoteAppStorage(fileSystemName, uri, context.getToken(), websocketPolicy);
                            RemoteTaskMonitor taskMonitor = createRemoteTaskMonitor(fileSystemName, uri, context.getToken());
                            return new AppFileSystem(fileSystemName, true, storage, taskMonitor);
                        })
                        .collect(Collectors.toList());
            } catch (ProcessingException e) {
                LOGGER.warn(e.toString());
                return Collections.emptyList();
            }
        } else {
            LOGGER.warn("Remote service config is missing");
            return Collections.emptyList();
        }
    }

    protected RemoteAppStorage createRemoteAppStorage(String fileSystemName, URI uri, String token, WebsocketConnectionPolicy websocketPolicy) {
        return new RemoteAppStorage(fileSystemName, uri, token, websocketPolicy);
    }

    protected RemoteTaskMonitor createRemoteTaskMonitor(String fileSystemName, URI uri, String token) {
        return new RemoteTaskMonitor(fileSystemName, uri, token);
    }
}
