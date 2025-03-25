package com.powsybl.afs;

import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.EventsBus;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.commons.util.ServiceLoaderCache;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class AppDataTest {
    @Mock
    private ComputationManager shortTimeExecutionComputationManager;
    @Mock
    private ComputationManager longTimeExecutionComputationManager;
    private AppData appDataUnderTest;

    @BeforeEach
    void init() {
        shortTimeExecutionComputationManager = mock(ComputationManager.class);
        longTimeExecutionComputationManager = mock(ComputationManager.class);
        appDataUnderTest = new AppData(shortTimeExecutionComputationManager, longTimeExecutionComputationManager);
    }

    @Test
    void setLongTimeExecutionComputationManager() {
        // GIVEN
        appDataUnderTest.setLongTimeExecutionComputationManager(null);
        Assertions.assertEquals(shortTimeExecutionComputationManager, appDataUnderTest.getLongTimeExecutionComputationManager());
        // WHEN
        appDataUnderTest.setLongTimeExecutionComputationManager(longTimeExecutionComputationManager);
        // THEN
        assertNotNull(appDataUnderTest.getLongTimeExecutionComputationManager());
    }

    @Test
    void setShortTimeExecutionComputationManager() {
        // GIVEN
        appDataUnderTest.setShortTimeExecutionComputationManager(null);
        Assertions.assertNull(appDataUnderTest.getShortTimeExecutionComputationManager());
        // WHEN
        appDataUnderTest.setShortTimeExecutionComputationManager(shortTimeExecutionComputationManager);
        // THEN
        assertNotNull(appDataUnderTest.getShortTimeExecutionComputationManager());
    }

    @Test
    void fileSystemProvidersTest() {
        // Create the expected list of AppFileSystem
        EventsBus eventsBus = new InMemoryEventsBus();
        AppFileSystemProviderContext context = new AppFileSystemProviderContext(shortTimeExecutionComputationManager, null, eventsBus);
        List<AppFileSystemProvider> appFileSystemProviders = new ServiceLoaderCache<>(AppFileSystemProvider.class).getServices();
        List<AppFileSystem> expectedAppFileSystems = new ArrayList<>();
        for (AppFileSystemProvider provider : appFileSystemProviders) {
            for (AppFileSystem fileSystem : provider.getFileSystems(context)) {
                fileSystem.setData(appDataUnderTest);
                expectedAppFileSystems.add(fileSystem);
            }
        }

        // Compare to the list in the AppData
        appDataUnderTest = new AppData(shortTimeExecutionComputationManager, longTimeExecutionComputationManager, appFileSystemProviders);
        List<AppFileSystem> fileSystems = appDataUnderTest.getFileSystems().stream().toList();
        assertThat(fileSystems.toArray()).containsExactlyInAnyOrderElementsOf(expectedAppFileSystems);
    }

    @Test
    void moreTest() {
        ComputationManager computationManager = Mockito.mock(ComputationManager.class);
        AppData appData = new AppData(computationManager, computationManager, Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        AppStorage storage = MapDbAppStorage.createMem("mem", appData.getEventsBus());
        AppFileSystem afs = new AppFileSystem("mem", false, storage);
        appData.addFileSystem(afs);
        afs.getRootFolder().createProject("test_project1");
        Folder testFolder = afs.getRootFolder().createFolder("test_folder");
        testFolder.createFolder("test_folder2");
        testFolder.createProject("test_project2");

        assertTrue(appData.getNode("mem:/").isPresent());
        assertTrue(appData.getNode("mem:/test_folder").isPresent());
        assertEquals(testFolder.getName(), appData.getNode("mem:/test_folder").get().getName());
        assertTrue(appData.getNode("mem:/test_folder:/test_folder2").isPresent());
        assertEquals(testFolder.getName(), appData.getNode("mem:/test_folder:/test_folder2").get().getName());
    }
}
