/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.powsybl.afs.ext.base.events.CaseImported;
import com.powsybl.afs.ext.base.events.ScriptModified;
import com.powsybl.afs.ext.base.events.VirtualCaseCreated;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
class EventsTest {

    String foopath = Paths.get("/tmp/foo").toString();

    @Test
    void caseImportedTest() throws IOException {
        CaseImported caseImported = new CaseImported("a", "b", foopath);
        assertEquals("a", caseImported.getId());
        assertEquals(CaseImported.TYPENAME, caseImported.getType());
        assertEquals("b", caseImported.getParentId());
        assertNotNull(caseImported.toString());
        assertEquals(foopath, caseImported.getPath());

        CaseImported caseImported2 = new CaseImported("a", "c", foopath);
        assertNotEquals(caseImported, caseImported2);
        assertNotEquals(caseImported.hashCode(), caseImported2.hashCode());
        assertNotEquals(caseImported, new ScriptModified("", "", foopath));
    }

    @Test
    void scriptModifiedTest() throws IOException {
        ScriptModified scriptModified = new ScriptModified("a", "b", foopath);
        assertEquals("a", scriptModified.getId());
        assertEquals(ScriptModified.TYPENAME, scriptModified.getType());
        assertEquals("b", scriptModified.getParentId());
        assertNotNull(scriptModified.toString());

        ScriptModified scriptModified2 = new ScriptModified("a", "c", foopath);
        assertNotEquals(scriptModified, scriptModified2);
        assertNotEquals(scriptModified.hashCode(), scriptModified2.hashCode());
        assertNotEquals(scriptModified, new CaseImported("", "", foopath));

    }

    @Test
    void virtualCaseCreatedTest() throws IOException {
        VirtualCaseCreated virtualCaseCreated = new VirtualCaseCreated("a", "b", foopath);
        assertEquals("a", virtualCaseCreated.getId());
        assertEquals(VirtualCaseCreated.TYPENAME, virtualCaseCreated.getType());
        assertEquals("b", virtualCaseCreated.getParentId());
        assertNotNull(virtualCaseCreated.toString());
        assertEquals(foopath, virtualCaseCreated.getPath());

        VirtualCaseCreated virtualCaseCreated2 = new VirtualCaseCreated("a", "c", foopath);
        assertNotEquals(virtualCaseCreated, virtualCaseCreated2);
        assertNotEquals(virtualCaseCreated.hashCode(), virtualCaseCreated2.hashCode());
        assertNotEquals(virtualCaseCreated, new CaseImported("", "", foopath));
    }
}
