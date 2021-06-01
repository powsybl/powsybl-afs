package com.powsybl.afs.timeseriesserver;

import com.google.auto.service.AutoService;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProvider;
import com.powsybl.afs.AppFileSystemProviderContext;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AbstractAppStorage;
import com.powsybl.afs.storage.EventsBus;
import com.powsybl.afs.timeseriesserver.storage.TimeSeriesServerAppStorage;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * @author amichaut@artelys.com
 */
@AutoService(AppFileSystemProvider.class)
public class TSServerAppFileSystemProvider implements AppFileSystemProvider {

    private final TSServerAppFileSystemConfig configuration;

    private final TimeSeriesServerAppStorage.TimeSeriesServerAppStorageProvider<URI, String, AbstractAppStorage, TimeSeriesServerAppStorage> storageProvider;

    private final MapDbAppStorage.MapDbAppStorageProvider<String, Path, EventsBus, MapDbAppStorage> delegateStorageProvider;

    public TSServerAppFileSystemProvider() {
        this(
                    TSServerAppFileSystemConfig.load(),
                    TimeSeriesServerAppStorage::new,
                    (name, path, eventsStore) -> MapDbAppStorage.createMmapFile(name, path.toFile(), eventsStore)
        );
    }

    public TSServerAppFileSystemProvider(final TSServerAppFileSystemConfig configuration,
                                         final TimeSeriesServerAppStorage.TimeSeriesServerAppStorageProvider<URI, String, AbstractAppStorage, TimeSeriesServerAppStorage> provider,
                                         final MapDbAppStorage.MapDbAppStorageProvider<String, Path, EventsBus, MapDbAppStorage> delegateStorageProvider) {
        this.configuration = configuration;
        this.storageProvider = provider;
        this.delegateStorageProvider = delegateStorageProvider;
    }

    @Override
    public List<AppFileSystem> getFileSystems(final AppFileSystemProviderContext context) {
        return Collections.singletonList(getFileSystem(context));
    }

    /**
     * @param context a context holding necessary event bus
     * @return a single time series server app file system, built using internal configuration and provided context
     */
    private TSServerAppFileSystem getFileSystem(final AppFileSystemProviderContext context) {
        // Make an URI form configuration
        URI uri;
        try {
            uri = configuration.getURI();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Could not build a proper target URI for time series server");
        }
        MapDbAppStorage delegateAppStorage = delegateStorageProvider.apply(configuration.getDriveName(), configuration.getDelegateConfig().getDbFile(), context.getEventsBus());
        return new TSServerAppFileSystem(
                    configuration.getDriveName(),
                    configuration.isRemotelyAccessible(),
                    storageProvider.apply(uri, configuration.getApp(), delegateAppStorage)
        );
    }
}
