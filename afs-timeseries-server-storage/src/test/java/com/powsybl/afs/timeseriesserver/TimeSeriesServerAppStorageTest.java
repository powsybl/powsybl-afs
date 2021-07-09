package com.powsybl.afs.timeseriesserver;

import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.timeseriesserver.storage.TimeSeriesServerAppStorage;
import org.junit.Before;

import java.net.URI;

public class TimeSeriesServerAppStorageTest extends AbstractAppStorageTest {

    private URI timeSeriesServerURI;

    private static final String AFS_APP = "AFS";

    public TimeSeriesServerAppStorageTest() {
        super(false, false);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        timeSeriesServerURI = new URI("http://localhost:9000/");
        super.setUp();
    }

    @Override
    protected AppStorage createStorage() {
        final MapDbAppStorage storage = MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
        return new TimeSeriesServerAppStorage(storage, timeSeriesServerURI, AFS_APP);
    }

}
