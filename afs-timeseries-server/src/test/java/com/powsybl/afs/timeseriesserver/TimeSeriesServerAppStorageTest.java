package com.powsybl.afs.timeseriesserver;

import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.AppStorage;
import org.junit.Before;

import java.net.URI;

public class TimeSeriesServerAppStorageTest extends AbstractAppStorageTest {

    private URI timeSeriesServerURI;

    @Override
    @Before
    public void setUp() throws Exception {
        timeSeriesServerURI = new URI("http://localhost:9000/");
        super.setUp();
    }

    @Override
    protected AppStorage createStorage() {
        return null;    //TODO
    }

}
