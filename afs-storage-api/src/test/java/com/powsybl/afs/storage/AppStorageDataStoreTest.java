/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Giovanni Ferrari <giovanni.ferrari at techrain.eu>
 */
public class AppStorageDataStoreTest {

    @Test
    public void fileNameTest() {
        AppStorageDataStore.FileName fileName = new AppStorageDataStore.FileName("test.xml");
        assertEquals("DATA_STORE_FILE_NAME__test.xml", fileName.toString());
        AppStorageDataStore.Name name = AppStorageDataStore.Name.parse("DATA_STORE_FILE_NAME__test.xml");
        assertTrue(name instanceof AppStorageDataStore.FileName);
        assertEquals("test.xml", ((AppStorageDataStore.FileName) name).getName());
    }
}
