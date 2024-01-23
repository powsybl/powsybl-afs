/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.scripting;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import org.junit.jupiter.api.Test;

import java.io.Reader;
import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class AfsExtensionErrorGroovyScriptTest extends AbstractGroovyScriptTest {

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    @Override
    protected Reader getCodeReader() {
        return new StringReader(String.join(System.lineSeparator(),
                "project = afs.getRootFolder('mem').createProject('test')",
                "bar = project.getRootFolder().barBuilder().build()"
        ));
    }

    @Override
    protected String getExpectedOutput() {
        return "";
    }

    @Override
    @Test
    public void test() {
        assertThrows(AfsException.class, super::test);
    }
}
