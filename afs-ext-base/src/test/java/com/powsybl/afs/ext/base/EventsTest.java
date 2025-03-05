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

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class EventsTest {
    private final Path path = Paths.get("/tmp/foo");

    @Test
    void caseImportedTest() {
        CaseImported caseImported = new CaseImported("a", "b", path.toString());
        assertEquals("a", caseImported.getId());
        assertEquals(CaseImported.TYPENAME, caseImported.getType());
        assertEquals("b", caseImported.getParentId());
        assertNotNull(caseImported.toString());
        assertEquals(path.toString(), caseImported.getPath());

        CaseImported caseImported2 = new CaseImported("a", "c", path.toString());
        assertNotEquals(caseImported, caseImported2);
        assertNotEquals(caseImported.hashCode(), caseImported2.hashCode());
        Object scriptModified = new ScriptModified("", "", path.toString());
        assertNotEquals(caseImported, scriptModified);
    }

    @Test
    void scriptModifiedTest() {
        ScriptModified scriptModified = new ScriptModified("a", "b", path.toString());
        assertEquals("a", scriptModified.getId());
        assertEquals(ScriptModified.TYPENAME, scriptModified.getType());
        assertEquals("b", scriptModified.getParentId());
        assertNotNull(scriptModified.toString());

        ScriptModified scriptModified2 = new ScriptModified("a", "c", path.toString());
        assertNotEquals(scriptModified, scriptModified2);
        assertNotEquals(scriptModified.hashCode(), scriptModified2.hashCode());
        Object caseImported = new CaseImported("", "", path.toString());
        assertNotEquals(caseImported, scriptModified);

    }

    @Test
    void virtualCaseCreatedTest() {
        VirtualCaseCreated virtualCaseCreated = new VirtualCaseCreated("a", "b", path.toString());
        assertEquals("a", virtualCaseCreated.getId());
        assertEquals(VirtualCaseCreated.TYPENAME, virtualCaseCreated.getType());
        assertEquals("b", virtualCaseCreated.getParentId());
        assertNotNull(virtualCaseCreated.toString());
        assertEquals(path.toString(), virtualCaseCreated.getPath());

        VirtualCaseCreated virtualCaseCreated2 = new VirtualCaseCreated("a", "c", path.toString());
        assertNotEquals(virtualCaseCreated, virtualCaseCreated2);
        assertNotEquals(virtualCaseCreated.hashCode(), virtualCaseCreated2.hashCode());
        Object caseImported = new CaseImported("", "", path.toString());
        assertNotEquals(caseImported, virtualCaseCreated);
    }
}
