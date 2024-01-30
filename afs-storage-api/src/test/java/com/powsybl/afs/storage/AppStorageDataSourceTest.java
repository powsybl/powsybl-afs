/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class AppStorageDataSourceTest {

    @Test
    public void suffixExtTest() {
        AppStorageDataSource.SuffixAndExtension suffixAndExtension = new AppStorageDataSource.SuffixAndExtension("_EQ", "xml");
        assertEquals("DATA_SOURCE_SUFFIX_EXT___EQ__xml", suffixAndExtension.toString());
        AppStorageDataSource.Name name = AppStorageDataSource.Name.parse("DATA_SOURCE_SUFFIX_EXT___EQ__xml");
        assertInstanceOf(AppStorageDataSource.SuffixAndExtension.class, name);
        assertEquals("_EQ", ((AppStorageDataSource.SuffixAndExtension) name).getSuffix());
        assertEquals("xml", ((AppStorageDataSource.SuffixAndExtension) name).getExt());
    }

    @Test
    public void fileNameTest() {
        AppStorageDataSource.FileName fileName = new AppStorageDataSource.FileName("test.xml");
        assertEquals("DATA_SOURCE_FILE_NAME__test.xml", fileName.toString());
        AppStorageDataSource.Name name = AppStorageDataSource.Name.parse("DATA_SOURCE_FILE_NAME__test.xml");
        assertTrue(name instanceof AppStorageDataSource.FileName);
        assertEquals("test.xml", ((AppStorageDataSource.FileName) name).getName());
    }
}
