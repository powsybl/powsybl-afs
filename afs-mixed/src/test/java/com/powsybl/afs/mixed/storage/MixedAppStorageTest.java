/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mixed.storage;

import com.powsybl.afs.AppData;
import com.powsybl.afs.mixed.MixedAppFileSystemConfig;
import com.powsybl.afs.storage.AppStorage;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class MixedAppStorageTest {

    @Test
    public void test() {
        MixedAppFileSystemConfig config = mock(MixedAppFileSystemConfig.class);
        when(config.getDriveName()).thenReturn("mixed");
        when(config.getDataStorageDriveName()).thenReturn("d");
        when(config.getNodeStorageDriveName()).thenReturn("n");
        AppStorage das = mock(AppStorage.class);
        AppStorage nas = mock(AppStorage.class);
        AppData appData = mock(AppData.class);
        when(appData.getRemotelyAccessibleStorage("d")).thenReturn(das);
        when(appData.getRemotelyAccessibleStorage("n")).thenReturn(nas);
        final MixedAppStorage mixedAppStorage = new MixedAppStorage(config);
        mixedAppStorage.setAppData(appData);
        mixedAppStorage.getChildNodes("a");
        verify(nas, times(1)).getChildNodes(eq("a"));
    }
}
