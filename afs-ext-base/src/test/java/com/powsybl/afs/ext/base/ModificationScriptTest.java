/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.google.common.collect.ImmutableList;
import com.powsybl.afs.*;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class ModificationScriptTest extends AbstractProjectFileTest {

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    @Override
    protected List<FileExtension> getFileExtensions() {
        return ImmutableList.of(new CaseExtension());
    }

    @Override
    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return ImmutableList.of(new ModificationScriptExtension(), new GenericScriptExtension());
    }

    @Test
    void test() {
        Project project = afs.getRootFolder().createProject("project");
        ProjectFolder rootFolder = project.getRootFolder();

        // create groovy script
        try {
            rootFolder.fileBuilder(ModificationScriptBuilder.class)
                    .withType(ScriptType.GROOVY)
                    .withContent("println 'hello'")
                    .build();
            fail();
        } catch (AfsException ignored) {
        }
        try {
            rootFolder.fileBuilder(ModificationScriptBuilder.class)
                    .withName("script")
                    .withContent("println 'hello'")
                    .build();
            fail();
        } catch (AfsException ignored) {
        }
        try {
            rootFolder.fileBuilder(ModificationScriptBuilder.class)
                    .withName("script")
                    .withType(ScriptType.GROOVY)
                    .build();
            fail();
        } catch (AfsException ignored) {
        }
        ModificationScript script = rootFolder.fileBuilder(ModificationScriptBuilder.class)
                .withName("script")
                .withType(ScriptType.GROOVY)
                .withContent("println 'hello'")
                .build();
        assertNotNull(script);
        assertEquals("script", script.getName());
        assertFalse(script.isFolder());
        assertTrue(script.getDependencies().isEmpty());
        assertEquals("println 'hello'", script.readScript());
        AtomicBoolean scriptUpdated = new AtomicBoolean(false);
        ScriptListener listener = () -> scriptUpdated.set(true);
        script.addListener(listener);
        script.writeScript("println 'bye'");
        assertEquals("println 'bye'", script.readScript());
        assertTrue(scriptUpdated.get());
        script.removeListener(listener);

        // check script file is correctly scanned
        assertEquals(1, rootFolder.getChildren().size());
        ProjectNode firstNode = rootFolder.getChildren().get(0);
        assertInstanceOf(ModificationScript.class, firstNode);
        assertEquals("script", firstNode.getName());

        ModificationScript include1 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
                .withName("include_script1")
                .withType(ScriptType.GROOVY)
                .withContent("var foo=\"bar\"")
                .build();
        assertNotNull(include1);
        script.addScript(include1);
        String contentWithInclude = script.readScript(true);
        assertEquals(contentWithInclude, "var foo=\"bar\"\n\nprintln 'bye'");

        script.addScript(include1);
        contentWithInclude = script.readScript(true);
        assertEquals(contentWithInclude, "var foo=\"bar\"\n\nvar foo=\"bar\"\n\nprintln 'bye'");

        ModificationScript include2 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
                .withName("include_script2")
                .withType(ScriptType.GROOVY)
                .withContent("var p0=1")
                .build();
        script.removeScript(include1.getId());
        script.addScript(include1);
        script.addScript(include2);
        contentWithInclude = script.readScript(true);
        assertEquals(contentWithInclude, "var foo=\"bar\"\n\nvar p0=1\n\nprintln 'bye'");

        ModificationScript include3 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
                .withName("include_script3")
                .withType(ScriptType.GROOVY)
                .withContent("var pmax=2")
                .build();
        script.addScript(include3);
        script.removeScript(include2.getId());
        contentWithInclude = script.readScript(true);
        assertEquals(contentWithInclude, "var foo=\"bar\"\n\nvar pmax=2\n\nprintln 'bye'");

        include3.addScript(include2);
        contentWithInclude = script.readScript(true);
        assertEquals(contentWithInclude, "var foo=\"bar\"\n\nvar p0=1\n\nvar pmax=2\n\nprintln 'bye'");

        List<AbstractScript> includes = script.getIncludedScripts();
        assertEquals(2, includes.size());
        assertEquals(includes.get(0).getId(), include1.getId());
        assertEquals(includes.get(1).getId(), include3.getId());

        assertThatCode(() -> script.addScript(script)).isInstanceOf(AfsCircularDependencyException.class);

        GenericScript genericScript = rootFolder.fileBuilder(GenericScriptBuilder.class)
                .withContent("some list")
                .withType(ScriptType.GROOVY)
                .withName("genericScript")
                .build();

        assertEquals("some list", genericScript.readScript());
        script.addGenericScript(genericScript);
        assertEquals("var foo=\"bar\"\n\nvar p0=1\n\nvar pmax=2\n\nsome list\n\nprintln 'bye'", script.readScript(true));
        assertThatCode(() -> genericScript.addGenericScript(genericScript)).isInstanceOf(AfsCircularDependencyException.class);
        script.removeScript(genericScript.getId());

        script.switchIncludedDependencies(0, 1);

        List<AbstractScript> includedScripts = script.getIncludedScripts();
        assertEquals(2, includes.size());
        assertEquals(includedScripts.get(0).getId(), include3.getId());
        assertEquals(includedScripts.get(1).getId(), include1.getId());

        assertThatCode(() -> script.switchIncludedDependencies(0, -1)).isInstanceOf(AfsException.class);
        assertThatCode(() -> script.switchIncludedDependencies(1, 2)).isInstanceOf(AfsException.class);

        assertThatCode(() -> rootFolder.fileBuilder(GenericScriptBuilder.class).build()).isInstanceOf(AfsException.class).hasMessage("Name is not set");
        assertThatCode(() -> rootFolder.fileBuilder(GenericScriptBuilder.class).withName("foo").build()).isInstanceOf(AfsException.class).hasMessage("Script type is not set");
        assertThatCode(() -> rootFolder.fileBuilder(GenericScriptBuilder.class).withName("foo").withType(ScriptType.GROOVY).build()).isInstanceOf(AfsException.class).hasMessage("Content is not set");
        assertThatCode(() -> rootFolder.fileBuilder(GenericScriptBuilder.class).withName("include_script2").withType(ScriptType.GROOVY).withContent("hello").build()).isInstanceOf(AfsException.class).hasMessage("Parent folder already contains a 'include_script2' node");

        //assert that the cache is correctly cleared
        assertEquals("include_script1", include1.getName());
        assertEquals("include_script3", include3.getName());
        assertEquals("include_script1", script.getIncludedScripts().get(1).getName());
        assertEquals("include_script3", script.getIncludedScripts().get(0).getName());

        include1.rename("include_script11");
        assertEquals("include_script11", include1.getName());
        assertNotEquals("include_script11", script.getIncludedScripts().get(1).getName());
        assertEquals("include_script1", script.getIncludedScripts().get(1).getName());

        script.clearDependenciesCache();
        assertEquals("include_script11", include1.getName());
        assertEquals("include_script11", script.getIncludedScripts().get(1).getName());
    }

    @Test
    void testModificationScriptCreationWithCustomPseudoClass() {
        // Create a project in the root folder
        Project project = afs.getRootFolder().createProject("project");

        // Define a custom pseudo-class value
        String customPseudoClass = "customPseudo";

        // Build a ModificationScript using the custom pseudo-class
        ModificationScript modificationScript = project.getRootFolder().fileBuilder(ModificationScriptBuilder.class)
                .withName("customScript")
                .withType(ScriptType.GROOVY)
                .withContent("println 'hello'")
                .withPseudoClass(customPseudoClass)
                .build();

        // Retrieve the node info for the created script from storage
        NodeInfo nodeInfo = storage.getChildNode(project.getRootFolder().getId(), "customScript")
                .orElseThrow(() -> new AssertionError("Node 'customScript' not found"));

        // Assert that the pseudo-class of the node is set to the custom value
        assertEquals(customPseudoClass, nodeInfo.getPseudoClass());
    }

    @Test
    void testModificationScriptCreationWithDefaultPseudoClass() {
        // Create a project in the root folder
        Project project = afs.getRootFolder().createProject("project");

        // Build a ModificationScript without specifying a pseudo-class, so the default should be used
        ModificationScript modificationScript = project.getRootFolder().fileBuilder(ModificationScriptBuilder.class)
                .withName("defaultScript")
                .withType(ScriptType.GROOVY)
                .withContent("println 'hello'")
                .build();

        // Retrieve the node info for the created script from storage
        NodeInfo nodeInfo = storage.getChildNode(project.getRootFolder().getId(), "defaultScript")
                .orElseThrow(() -> new AssertionError("Node 'defaultScript' not found"));

        // Assert that the pseudo-class of the node is set to the default value defined in ModificationScript.PSEUDO_CLASS
        assertEquals(ModificationScript.PSEUDO_CLASS, nodeInfo.getPseudoClass());
    }



}
