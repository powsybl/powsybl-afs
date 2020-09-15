/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author Giovanni Ferrari <giovanni.ferrari at techrain.eu>
 */
public class AppStorageDataStoreTest {

    @Test
    public void fileNameTest() {
        assertEquals("DATA_STORE_ENTRY_NAME__test.xml", AppStorageDataStore.getEntryDataName("test.xml"));

    }
}
