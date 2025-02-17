/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.powsybl.afs.AbstractProjectFileTest;
import com.powsybl.afs.AfsCircularDependencyException;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.FileExtension;
import com.powsybl.afs.Project;
import com.powsybl.afs.ProjectFileExtension;
import com.powsybl.afs.ProjectFolder;
import com.powsybl.afs.ProjectNode;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class ModificationScriptTest extends AbstractProjectFileTest {

    private Project project;
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
        project = afs.getRootFolder().createProject("project");
        rootFolder = project.getRootFolder();
    }

    @Test
    void buildingScriptExceptionsTest() {
        ModificationScriptBuilder builder;
        AfsException exception;

        // Missing name
        builder = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withType(ScriptType.GROOVY)
            .withContent("println 'hello'");
        exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Name is not set", exception.getMessage());

        // Missing Type
        builder = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withContent("println 'hello'");
        exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Script type is not set", exception.getMessage());

        // Missing Content
        builder = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY);
        exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Content is not set", exception.getMessage());
    }

    @Test
    void createScriptTest() {
        // Create script
        ModificationScript script = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("println 'hello'")
            .build();

        // Check creation
        assertNotNull(script);
        assertEquals("script", script.getName());
        assertFalse(script.isFolder());
        assertTrue(script.getDependencies().isEmpty());
        assertEquals("println 'hello'", script.readScript());
    }

    @Test
    void listenerTest() {
        ModificationScript script = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("println 'hello'")
            .build();

        // Add listener
        AtomicBoolean scriptUpdated = new AtomicBoolean(false);
        ScriptListener listener = () -> scriptUpdated.set(true);
        script.addListener(listener);

        // Update the script
        script.writeScript("println 'bye'");

        // Check the update
        assertEquals("println 'bye'", script.readScript());
        assertTrue(scriptUpdated.get());
    }

    @Test
    void scriptDetectedInFolder() {
        rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("println 'hello'")
            .build();

        // Check in root folder
        assertEquals(1, rootFolder.getChildren().size());
        ProjectNode firstNode = rootFolder.getChildren().get(0);
        assertInstanceOf(ModificationScript.class, firstNode);
        assertEquals("script", firstNode.getName());
    }

    @Test
    void includesTest() {
        ModificationScript script = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("println 'bye'")
            .build();

        // Create some scripts to include
        ModificationScript include1 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script1")
            .withType(ScriptType.GROOVY)
            .withContent("var foo=\"bar\"")
            .build();
        ModificationScript include2 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script2")
            .withType(ScriptType.GROOVY)
            .withContent("var p0=1")
            .build();
        ModificationScript include3 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script3")
            .withType(ScriptType.GROOVY)
            .withContent("var pmax=2")
            .build();

        // Add the first one
        script.addScript(include1);
        String contentWithInclude = script.readScript(true);
        assertEquals("var foo=\"bar\"\n\nprintln 'bye'", contentWithInclude);

        // Include it a second time
        script.addScript(include1);
        contentWithInclude = script.readScript(true);
        assertEquals("var foo=\"bar\"\n\nvar foo=\"bar\"\n\nprintln 'bye'", contentWithInclude);

        // Remove the first and add it one time and then the second one
        script.removeScript(include1.getId());
        script.addScript(include1);
        script.addScript(include2);
        contentWithInclude = script.readScript(true);
        assertEquals("var foo=\"bar\"\n\nvar p0=1\n\nprintln 'bye'", contentWithInclude);

        // Add the third and remove the second
        script.addScript(include3);
        script.removeScript(include2.getId());
        contentWithInclude = script.readScript(true);
        assertEquals("var foo=\"bar\"\n\nvar pmax=2\n\nprintln 'bye'", contentWithInclude);

        // Add the second again but in the third one
        include3.addScript(include2);
        contentWithInclude = script.readScript(true);
        assertEquals("var foo=\"bar\"\n\nvar p0=1\n\nvar pmax=2\n\nprintln 'bye'", contentWithInclude);

        // List of included scripts
        List<AbstractScript> includes = script.getIncludedScripts();
        assertEquals(2, includes.size());
        assertEquals(include1.getId(), includes.get(0).getId());
        assertEquals(include3.getId(), includes.get(1).getId());
    }

    @Test
    void circularInclusionTest() {
        ModificationScript script = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("println 'hello'")
            .build();

        // Include the script in itself
        AfsCircularDependencyException exception = assertThrows(AfsCircularDependencyException.class, () -> script.addScript(script));
        assertEquals("Circular dependency detected", exception.getMessage());
    }

    @Test
    void includeGenericScriptTest() {
        ModificationScript script = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("println 'bye'")
            .build();
        ModificationScript include1 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script1")
            .withType(ScriptType.GROOVY)
            .withContent("var foo=\"bar\"")
            .build();
        ModificationScript include2 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script2")
            .withType(ScriptType.GROOVY)
            .withContent("var p0=1")
            .build();
        ModificationScript include3 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script3")
            .withType(ScriptType.GROOVY)
            .withContent("var pmax=2")
            .build();
        script.addScript(include1);
        script.addScript(include3);
        include3.addScript(include2);

        // Create a generic script
        GenericScript genericScript = rootFolder.fileBuilder(GenericScriptBuilder.class)
            .withContent("some list")
            .withType(ScriptType.GROOVY)
            .withName("genericScript")
            .build();
        assertEquals("some list", genericScript.readScript());

        // Include the generic script to the script
        script.addGenericScript(genericScript);
        assertEquals("var foo=\"bar\"\n\nvar p0=1\n\nvar pmax=2\n\nsome list\n\nprintln 'bye'", script.readScript(true));
    }

    @Test
    void genericScriptCircularDependencyTest() {
        GenericScript genericScript = rootFolder.fileBuilder(GenericScriptBuilder.class)
            .withContent("some list")
            .withType(ScriptType.GROOVY)
            .withName("genericScript")
            .build();

        // Include the script in itself
        AfsCircularDependencyException exception = assertThrows(AfsCircularDependencyException.class, () -> genericScript.addGenericScript(genericScript));
        assertEquals("Circular dependency detected", exception.getMessage());
    }

    @Test
    void switchDependencyTest() {
        ModificationScript script = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("println 'bye'")
            .build();
        ModificationScript include1 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script1")
            .withType(ScriptType.GROOVY)
            .withContent("var foo=\"bar\"")
            .build();
        ModificationScript include2 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script2")
            .withType(ScriptType.GROOVY)
            .withContent("var p0=1")
            .build();
        ModificationScript include3 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script3")
            .withType(ScriptType.GROOVY)
            .withContent("var pmax=2")
            .build();
        script.addScript(include1);
        script.addScript(include3);
        include3.addScript(include2);

        // Switch the two included scripts
        script.switchIncludedDependencies(0, 1);
        String contentWithInclude = script.readScript(true);
        assertEquals("var p0=1\n\nvar pmax=2\n\nvar foo=\"bar\"\n\nprintln 'bye'", contentWithInclude);

        // List of included scripts
        List<AbstractScript> includes = script.getIncludedScripts();
        assertEquals(2, includes.size());
        assertEquals(include3.getId(), includes.get(0).getId());
        assertEquals(include1.getId(), includes.get(1).getId());
    }

    @Test
    void switchDependencyExceptionsTest() {
        ModificationScript script = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("println 'bye'")
            .build();
        ModificationScript include1 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script1")
            .withType(ScriptType.GROOVY)
            .withContent("var foo=\"bar\"")
            .build();
        ModificationScript include2 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script2")
            .withType(ScriptType.GROOVY)
            .withContent("var p0=1")
            .build();
        ModificationScript include3 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script3")
            .withType(ScriptType.GROOVY)
            .withContent("var pmax=2")
            .build();
        script.addScript(include1);
        script.addScript(include3);
        include3.addScript(include2);

        // Switch non-existing dependencies
        AfsException exception = assertThrows(AfsException.class, () -> script.switchIncludedDependencies(0, -1));
        assertEquals("One or both indexes values are out of bounds", exception.getMessage());
        exception = assertThrows(AfsException.class, () -> script.switchIncludedDependencies(1, 2));
        assertEquals("One or both indexes values are out of bounds", exception.getMessage());
    }

    @Test
    void renameAndCacheTest() {
        ModificationScript script = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("println 'bye'")
            .build();
        ModificationScript include1 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script1")
            .withType(ScriptType.GROOVY)
            .withContent("var foo=\"bar\"")
            .build();
        ModificationScript include2 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script2")
            .withType(ScriptType.GROOVY)
            .withContent("var p0=1")
            .build();
        ModificationScript include3 = rootFolder.fileBuilder(ModificationScriptBuilder.class)
            .withName("include_script3")
            .withType(ScriptType.GROOVY)
            .withContent("var pmax=2")
            .build();
        script.addScript(include3);
        script.addScript(include1);
        include3.addScript(include2);

        // Original names
        assertEquals("include_script1", include1.getName());
        assertEquals("include_script3", include3.getName());
        assertEquals("include_script1", script.getIncludedScripts().get(1).getName());
        assertEquals("include_script3", script.getIncludedScripts().get(0).getName());

        // Rename a script without clearing the cache - the name change is not taken into account
        include1.rename("include_script11");
        assertEquals("include_script11", include1.getName());
        assertNotEquals("include_script11", script.getIncludedScripts().get(1).getName());
        assertEquals("include_script1", script.getIncludedScripts().get(1).getName());

        // Clear the cache - the name change is taken into account
        script.clearDependenciesCache();
        assertEquals("include_script11", include1.getName());
        assertEquals("include_script11", script.getIncludedScripts().get(1).getName());
    }

    @Test
    void testModificationScriptCreationWithCustomPseudoClass() {
        // Define a custom pseudo-class value
        String customPseudoClass = "customPseudo";

        // Build a ModificationScript using the custom pseudo-class
        project.getRootFolder().fileBuilder(ModificationScriptBuilder.class)
            .withName("customScript")
            .withType(ScriptType.GROOVY)
            .withContent("script content")
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
        // Build a ModificationScript without specifying a pseudo-class, so the default should be used
        project.getRootFolder().fileBuilder(ModificationScriptBuilder.class)
            .withName("defaultScript")
            .withType(ScriptType.GROOVY)
            .withContent("script content")
            .build();

        // Retrieve the node info for the created script from storage
        NodeInfo nodeInfo = storage.getChildNode(project.getRootFolder().getId(), "defaultScript")
            .orElseThrow(() -> new AssertionError("Node 'defaultScript' not found"));

        // Assert that the pseudo-class of the node is set to the default value defined in ModificationScript.PSEUDO_CLASS
        assertEquals(ModificationScript.PSEUDO_CLASS, nodeInfo.getPseudoClass());
    }

}
