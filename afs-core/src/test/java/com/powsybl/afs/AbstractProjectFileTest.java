/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.powsybl.afs.storage.AppStorage;
import com.powsybl.computation.ComputationManager;
import com.powsybl.computation.local.LocalComputationManager;
import com.powsybl.iidm.network.Network;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public abstract class AbstractProjectFileTest {

    protected AppStorage storage;

    protected AppFileSystem afs;

    protected AppData ad;

    protected Network network;

    protected abstract AppStorage createStorage();

    protected List<FileExtension> getFileExtensions() {
        return Collections.emptyList();
    }

    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return Collections.emptyList();
    }

    protected List<ServiceExtension> getServiceExtensions() {
        return Collections.emptyList();
    }

    @BeforeEach
    public void setup() throws IOException {
        network = Network.create("test", "test");
        network.newSubstation()
            .setId("s1")
            .setTso("TSO")
            .add();
        ComputationManager computationManager = new LocalComputationManager();
        storage = createStorage();
        afs = new AppFileSystem("mem", false, storage);
        ad = new AppData(computationManager, computationManager,
            Collections.singletonList(computationManager1 -> Collections.singletonList(afs)),
            getFileExtensions(),
            getProjectFileExtensions(),
            getServiceExtensions());
        afs.setData(ad);
    }

    @AfterEach
    public void tearDown() throws IOException {
        storage.close();
    }
}
