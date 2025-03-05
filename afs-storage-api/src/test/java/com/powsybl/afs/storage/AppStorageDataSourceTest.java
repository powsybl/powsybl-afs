/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import com.powsybl.commons.datasource.DataSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class AppStorageDataSourceTest {

    @Test
    void suffixExtTest() {
        AppStorageDataSource.SuffixAndExtension suffixAndExtension = new AppStorageDataSource.SuffixAndExtension("_EQ", "xml");
        assertEquals("DATA_SOURCE_SUFFIX_EXT___EQ__xml", suffixAndExtension.toString());
        AppStorageDataSource.Name name = AppStorageDataSource.Name.parse("DATA_SOURCE_SUFFIX_EXT___EQ__xml");
        assertInstanceOf(AppStorageDataSource.SuffixAndExtension.class, name);
        assertEquals("_EQ", ((AppStorageDataSource.SuffixAndExtension) name).getSuffix());
        assertEquals("xml", ((AppStorageDataSource.SuffixAndExtension) name).getExt());
    }

    @Test
    void fileNameTest() {
        AppStorageDataSource.FileName fileName = new AppStorageDataSource.FileName("test.xml");
        assertEquals("DATA_SOURCE_FILE_NAME__test.xml", fileName.toString());
        AppStorageDataSource.Name name = AppStorageDataSource.Name.parse("DATA_SOURCE_FILE_NAME__test.xml");
        assertInstanceOf(AppStorageDataSource.FileName.class, name);
        assertEquals("test.xml", ((AppStorageDataSource.FileName) name).getName());
    }

    @Test
    void outputStreamByFileNameExceptionTest() {
        try (AppStorage storage = mock(AppStorage.class)) {
            DataSource ds = new AppStorageDataSource(storage, "nodeId", "nodeName");
            try (OutputStream ignored = ds.newOutputStream("fileName", true)) {
                fail();
            } catch (UnsupportedOperationException e) {
                assertEquals("Append mode not supported", e.getMessage());
            } catch (IOException e) {
                fail();
            }
        }
    }

    @Test
    void outputStreamByBaseNameAndExtensionExceptionTest() {
        try (AppStorage storage = mock(AppStorage.class)) {
            DataSource ds = new AppStorageDataSource(storage, "nodeId", "nodeName");
            try (OutputStream ignored = ds.newOutputStream("baseName", "extension", true)) {
                fail();
            } catch (UnsupportedOperationException e) {
                assertEquals("Append mode not supported", e.getMessage());
            } catch (IOException e) {
                fail();
            }
        }
    }
}
