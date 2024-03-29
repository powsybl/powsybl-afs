/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage.events;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author Chamseddine Benhamed {@literal <Chamseddine.Benhamed at rte-france.com>}
 */
class NodeEventContainerTest {

    @Test
    void mainTest() {
        NodeEventContainer nodeEventContainer = new NodeEventContainer(new NodeCreated("id", "parentid"), "fs", "topic");
        assertEquals("fs", nodeEventContainer.getFileSystemName());
        assertEquals("topic", nodeEventContainer.getTopic());
        assertEquals(nodeEventContainer.getNodeEvent(), new NodeCreated("id", "parentid"));

        NodeEventContainer nodeEventContainer1 = new NodeEventContainer();
        assertNull(nodeEventContainer1.getNodeEvent());
        assertNull(nodeEventContainer1.getTopic());
        assertNull(nodeEventContainer1.getFileSystemName());
    }
}
