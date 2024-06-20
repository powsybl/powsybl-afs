/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class DependencyCacheTest extends AbstractProjectFileTest {

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    @Override
    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return List.of(new FooFileExtension(), new WithDependencyFileExtension());
    }

    @Test
    void test() {
        Project project = afs.getRootFolder().createProject("project");
        FooFile tic = project.getRootFolder().fileBuilder(FooFileBuilder.class).withName("tic").build();
        FooFile tic2 = project.getRootFolder().fileBuilder(FooFileBuilder.class).withName("tic2").build();
        WithDependencyFile tac = project.getRootFolder().fileBuilder(WithDependencyFileBuilder.class).withName("WithDependencyFile").build();
        assertNull(tac.getTicDependency());
        tac.setFooDependency(tic);
        assertNotNull(tac.getTicDependency());
        assertEquals(tic.getId(), tac.getTicDependency().getId());
        tac.setFooDependency(tic2);
        assertNotNull(tac.getTicDependency());
        assertEquals(tic2.getId(), tac.getTicDependency().getId());
    }
}
