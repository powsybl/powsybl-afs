/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ext.base;

import com.powsybl.afs.AbstractProjectFileTest;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.FileExtension;
import com.powsybl.afs.Project;
import com.powsybl.afs.ProjectFileExtension;
import com.powsybl.afs.ProjectFolder;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
public class GenericScriptTest extends AbstractProjectFileTest {

    private ProjectFolder rootFolder;

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    @Override
    protected List<FileExtension> getFileExtensions() {
        return List.of(new CaseExtension());
    }

    @Override
    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return List.of(new ModificationScriptExtension(), new GenericScriptExtension());
    }

    @Override
    @BeforeEach
    public void setup() throws IOException {
        super.setup();
        Project project = afs.getRootFolder().createProject("project");
        rootFolder = project.getRootFolder();
    }

    @Test
    void buildingScriptExceptionsTest() {
        GenericScriptBuilder builder;
        AfsException exception;

        // Missing name
        builder = rootFolder.fileBuilder(GenericScriptBuilder.class)
            .withType(ScriptType.GROOVY)
            .withContent("println 'hello'");
        exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Name is not set", exception.getMessage());

        // Missing Type
        builder = rootFolder.fileBuilder(GenericScriptBuilder.class)
            .withName("script")
            .withContent("println 'hello'");
        exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Script type is not set", exception.getMessage());

        // Missing Content
        builder = rootFolder.fileBuilder(GenericScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY);
        exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Content is not set", exception.getMessage());
    }
}
