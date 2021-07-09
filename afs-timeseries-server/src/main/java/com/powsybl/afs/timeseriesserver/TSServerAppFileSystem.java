package com.powsybl.afs.timeseriesserver;

import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.storage.AppStorage;

/**
 * @author amichaut@artelys.com
 */
public class TSServerAppFileSystem extends AppFileSystem {

    public TSServerAppFileSystem(final String name, final boolean remotelyAccessible, final AppStorage storage) {
        super(name, remotelyAccessible, storage);
    }
}
