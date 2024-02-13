/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.action.dsl;

import com.google.common.collect.ImmutableList;
import com.powsybl.afs.AbstractProjectFileTest;
import com.powsybl.afs.Project;
import com.powsybl.afs.ProjectFileExtension;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.contingency.Contingency;
import com.powsybl.contingency.LineContingency;
import com.powsybl.iidm.network.Line;
import com.powsybl.iidm.network.Network;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class ActionScriptTest extends AbstractProjectFileTest {

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    @Override
    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return ImmutableList.of(new ActionScriptExtension());
    }

    @Test
    void test() {
        // create project in the root folder
        Project project = afs.getRootFolder().createProject("project");

        // create contingency list
        ActionScript actionScript = project.getRootFolder().fileBuilder(ActionScriptBuilder.class)
                .withName("contingencies")
                .withContent(String.join(System.lineSeparator(),
                        "contingency('c1') {",
                        "    equipments 'l1'",
                        "}",
                        ""))
                .build();
        List<Contingency> contingencies = Collections.singletonList(new Contingency("c1", new LineContingency("l1")));

        Network network = Mockito.mock(Network.class);
        Mockito.when((Line) network.getIdentifiable("l1")).thenReturn(Mockito.mock(Line.class));

        Assertions.assertThat(contingencies).hasSameElementsAs(actionScript.getContingencies(network));

    }
}
