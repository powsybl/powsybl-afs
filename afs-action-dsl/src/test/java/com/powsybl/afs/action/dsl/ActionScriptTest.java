/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.action.dsl;

import com.powsybl.afs.AbstractProjectFileTest;
import com.powsybl.afs.Project;
import com.powsybl.afs.ProjectFileExtension;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.contingency.Contingency;
import com.powsybl.contingency.LineContingency;
import com.powsybl.iidm.network.Line;
import com.powsybl.iidm.network.Network;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        return List.of(new ActionScriptExtension());
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

        assertThat(contingencies)
            .hasSameElementsAs(actionScript.getContingencies(network))
            .hasSameElementsAs(actionScript.getContingencies(network, new HashMap<>()));

    }

    @Test
    void testActionScripCreationWithCustomPseudoClass() {
        // Create a project in the root folder
        Project project = afs.getRootFolder().createProject("project");

        // Define a custom pseudo-class value
        String customPseudoClass = "customPseudo";

        // Build an ActionScript using the custom pseudo-class
        project.getRootFolder().fileBuilder(ActionScriptBuilder.class)
            .withName("customScript")
            .withContent("script content")
            .withPseudoClass(customPseudoClass)
            .build();

        // Retrieve the node info for the created script from storage
        NodeInfo nodeInfo = storage
            .getChildNode(project.getRootFolder().getId(), "customScript")
            .orElseThrow(() -> new AssertionError("Node 'customScript' not found"));

        // Assert that the pseudo-class of the node is set to the custom value
        assertEquals(customPseudoClass, nodeInfo.getPseudoClass());
    }

    @Test
    void testActionScripCreationWithDefaultPseudoClass() {
        // Create a project in the root folder
        Project project = afs.getRootFolder().createProject("project");

        // Build an ActionScript without specifying a pseudo-class
        project.getRootFolder().fileBuilder(ActionScriptBuilder.class)
            .withName("defaultScript")
            .withContent("script content")
            .build();

        // Retrieve the node info for the created script from storage
        NodeInfo nodeInfo = storage
            .getChildNode(project.getRootFolder().getId(), "defaultScript")
            .orElseThrow(() -> new AssertionError("Node 'defaultScript' not found"));

        // Assert that the pseudo-class of the node is set to the default value defined in ActionScript.PSEUDO_CLASS
        assertEquals(ActionScript.PSEUDO_CLASS, nodeInfo.getPseudoClass());
    }

}
