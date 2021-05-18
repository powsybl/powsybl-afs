package com.powsybl.afs.timeseriesserver;

import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import org.junit.Before;

import java.net.URI;

public class TimeSeriesServerAppStorageTest extends AbstractAppStorageTest {

    private URI timeSeriesServerURI;

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
        return new TimeSeriesServerAppStorage(MapDbAppStorage.createMem("mem", new InMemoryEventsBus()), timeSeriesServerURI);
    }

}
