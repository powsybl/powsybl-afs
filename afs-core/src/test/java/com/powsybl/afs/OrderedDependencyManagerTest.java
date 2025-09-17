/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs;


import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Matthieu SAUR {@literal <matthieu.saur at rte-france.com>}
 */
class OrderedDependencyManagerTest {

    private AppFileSystem afs;

    @BeforeEach
    void setup() {
        ComputationManager computationManager = Mockito.mock(ComputationManager.class);
        AppData appData = new AppData(computationManager, computationManager, Collections.emptyList(),
                Collections.emptyList(), List.of(new FooFileExtension(), new WithDependencyFileExtension()), Collections.emptyList());

        AppStorage storage = MapDbAppStorage.createMem("mem", appData.getEventsBus());

        afs = new AppFileSystem("mem", true, storage);
        afs.setData(appData);
        appData.addFileSystem(afs);
    }

    @Test
    void removeAllDependenciesTest() {
        // GIVEN
        String includedScriptName = "IncludedScript";
        String includedDataTableName = "IncludedDataTable";

        Folder folder = afs.getRootFolder().createFolder("testFolder");
        Project project = folder.createProject("test");
        FooFile fooFile = project.getRootFolder().fileBuilder(FooFileBuilder.class).withName("Foo").build();

        WithDependencyFile fileWithDep = project.getRootFolder().fileBuilder(WithDependencyFileBuilder.class).withName("WithDependencyFile1").build();
        fileWithDep.setFooDependency(fooFile);
        ProjectFile includedNode = fooFile.invalidate().getFirst();
        OrderedDependencyManager orderedDependencyManager = new OrderedDependencyManager(fooFile.invalidate().getFirst());
        orderedDependencyManager.appendDependencies(includedScriptName, Collections.singletonList(includedNode));
        orderedDependencyManager.appendDependencies(includedDataTableName, Collections.singletonList(includedNode));
        // WHEN
        orderedDependencyManager.removeAllDependencies(includedScriptName);
        // THEN
        List<ProjectNode> includedScripts = orderedDependencyManager.getDependencies(includedScriptName);
        assertThat(includedScripts).isEmpty();
        List<ProjectNode> includedDataTables = orderedDependencyManager.getDependencies(includedDataTableName);
        assertThat(includedDataTables).isNotEmpty();
    }
}
