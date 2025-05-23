/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.contingency;

import com.powsybl.afs.AbstractProjectFileTest;
import com.powsybl.afs.Project;
import com.powsybl.afs.ProjectFileExtension;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.contingency.BranchContingency;
import com.powsybl.contingency.Contingency;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class ContingencyStoreTest extends AbstractProjectFileTest {

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    @Override
    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return List.of(new ContingencyStoreExtension());
    }

    @Test
    void test() {
        // create project in the root folder
        Project project = afs.getRootFolder().createProject("project");
        storage.setConsistent(project.getId());

        // create contingency list
        ContingencyStore contingencyStore = project.getRootFolder().fileBuilder(ContingencyStoreBuilder.class)
                .withName("contingencies")
                .build();
        List<Contingency> contingencies = Collections.singletonList(new Contingency("c1", new BranchContingency("l1")));
        contingencyStore.write(contingencies);
        assertEquals(contingencies, contingencyStore.read());
        assertEquals(new ArrayList<>(), contingencyStore.getContingencies(network));
    }
}
