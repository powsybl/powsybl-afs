package com.powsybl.afs.timeseriesserver;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.AppFileSystemProviderContext;
import com.powsybl.afs.mapdb.MapDbAppFileSystemConfig;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AbstractAppStorage;
import com.powsybl.afs.storage.EventsBus;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.timeseriesserver.storage.TimeSeriesServerAppStorage;
import com.powsybl.computation.ComputationManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.MediaType;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.junit.Assert.assertEquals;

public class TSServerAppFileSystemProviderTest {

    private static final int SRV_PORT = 9876;
    public static final String DRIVE = "drive";

    private TSServerAppFileSystemConfig config;
    private MapDbAppFileSystemConfig delegateConfig;

    /**
     * Mock server to simulate time series provider
     */
    private ClientAndServer mockServer;

    /**
     * In-memory fs for mapDB
     */
    private FileSystem fileSystem;

    /**
     * mapDB file
     */
    private Path dbFile;

    @Before
    public void setUp() {
        mockServer = ClientAndServer.startClientAndServer(SRV_PORT);
        setupMockServer();

        // Setup connection parameters
        config = new TSServerAppFileSystemConfig(DRIVE, true);
        config.setScheme("http");
        config.setHost("localhost");
        config.setPort(mockServer.getPort());

        // Setup delegate config
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        dbFile = fileSystem.getPath("/db");
        delegateConfig = new MapDbAppFileSystemConfig(DRIVE, true, dbFile);
        config.setDelegateConfig(delegateConfig);
    }

    /**
     * Setup mock server for tests
     */
    private void setupMockServer() {
        mockServer.when(request()
                    .withMethod("GET")
                    .withPath("/v1/timeseries/apps")
        ).respond(response()
                    .withStatusCode(200)
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withBody("[]")
        );
        mockServer.when(request()
                    .withMethod("POST")
                    .withPath("/v1/timeseries/apps/.*")
        ).respond(response()
                    .withStatusCode(200)
                    .withContentType(MediaType.APPLICATION_JSON)
        );
    }

    @After
    public void tearDown() throws IOException {
        mockServer.stop();
        fileSystem.close();
    }

    @Test
    public void provideTest() {
        // Build a mock computation context
        ComputationManager computationManager = Mockito.mock(ComputationManager.class);
        final AppFileSystemProviderContext context = new AppFileSystemProviderContext(computationManager, null, new InMemoryEventsBus());
        // Build a new provider
        final MapDbAppStorage.MapDbAppStorageProvider<String, Path, EventsBus, MapDbAppStorage> delegateAppStorageProvider = (name, path, eventsStore) -> MapDbAppStorage.createMem(name, eventsStore);
        final TimeSeriesServerAppStorage.TimeSeriesServerAppStorageProvider<URI, String, AbstractAppStorage, TimeSeriesServerAppStorage> appStorageProvider = TimeSeriesServerAppStorage::new;
        TSServerAppFileSystemProvider provider = new TSServerAppFileSystemProvider(config, appStorageProvider, delegateAppStorageProvider);
        // Check that FS is correct
        final List<AppFileSystem> fileSystems = provider.getFileSystems(context);
        assertEquals(fileSystems.size(), 1);
        assertTrue(fileSystems.get(0) instanceof TSServerAppFileSystem);
        final TSServerAppFileSystem fs = (TSServerAppFileSystem) fileSystems.get(0);
        assertEquals(fs.getName(), DRIVE);
        assertTrue(fs.isRemotelyAccessible());
    }
}
