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
import com.powsybl.afs.Folder;
import com.powsybl.afs.Project;
import com.powsybl.afs.ProjectFile;
import com.powsybl.afs.ProjectFileExtension;
import com.powsybl.afs.ProjectFolder;
import com.powsybl.afs.ServiceExtension;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.iidm.network.DefaultNetworkListener;
import com.powsybl.iidm.network.ImportConfig;
import com.powsybl.iidm.network.ImportersLoader;
import com.powsybl.iidm.network.ImportersLoaderList;
import com.powsybl.iidm.network.NetworkListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class VirtualCaseTest extends AbstractProjectFileTest {

    private Case aCase;
    private ProjectFolder folder;
    private ImportedCase importedCase;

    private ImportersLoader createImportersLoader() {
        return new ImportersLoaderList(new TestImporter(network));
    }

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    @Override
    protected List<FileExtension> getFileExtensions() {
        return List.of(new CaseExtension(createImportersLoader()));
    }

    @Override
    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return List.of(new ImportedCaseExtension(createImportersLoader(), new ImportConfig()),
            new ModificationScriptExtension(),
            new VirtualCaseExtension());
    }

    @Override
    protected List<ServiceExtension> getServiceExtensions() {
        return List.of(new LocalNetworkCacheServiceExtension());
    }

    @BeforeEach
    @Override
    public void setup() throws IOException {
        super.setup();
        NodeInfo rootFolderInfo = storage.createRootNodeIfNotExists("root", Folder.PSEUDO_CLASS);
        NodeInfo nodeInfo = storage.createNode(rootFolderInfo.getId(), "network", Case.PSEUDO_CLASS, "", Case.VERSION,
            new NodeGenericMetadata().setString(Case.FORMAT, TestImporter.FORMAT));
        storage.setConsistent(nodeInfo.getId());

        // get case
        aCase = (Case) afs.getRootFolder().getChildren().get(0);

        // create project
        Project project = afs.getRootFolder().createProject("project");

        // create project folder
        folder = project.getRootFolder().createFolder("folder");

        // import case into project
        importedCase = folder.fileBuilder(ImportedCaseBuilder.class)
            .withCase(aCase)
            .build();
    }

    @Test
    void buildingVirtualCaseExceptionsTest() {
        VirtualCaseBuilder builder;
        AfsException exception;

        // Create groovy script
        ModificationScript script = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("print 'hello'")
            .build();

        // Missing name
        builder = folder.fileBuilder(VirtualCaseBuilder.class)
            .withCase(importedCase)
            .withScript(script);
        exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Name is not set", exception.getMessage());

        // Missing Type
        builder = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network2")
            .withScript(script);
        exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Case is not set", exception.getMessage());

        // Missing Content
        builder = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network2")
            .withCase(importedCase);
        exception = assertThrows(AfsException.class, builder::build);
        assertEquals("Script is not set", exception.getMessage());
    }

    @Test
    void buildingVirtualCaseTest() {
        // create groovy script
        ModificationScript script = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("print 'hello'")
            .build();

        // Build the virtual case
        VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network2")
            .withCase(importedCase)
            .withScript(script)
            .build();

        // Checks on the virtual case
        assertEquals("network2", virtualCase.getName());
        assertTrue(virtualCase.getCase().isPresent());
        assertTrue(virtualCase.getScript().isPresent());
        assertEquals(2, virtualCase.getDependencies().size());
        assertEquals(1, importedCase.getBackwardDependencies().size());
        assertEquals(1, script.getBackwardDependencies().size());
        assertNotNull(virtualCase.getNetwork());
        assertFalse(virtualCase.mandatoryDependenciesAreMissing());
        assertEquals("hello", virtualCase.getOutput());
    }

    @Test
    void invalidateCacheOnUpdateTest() {
        ModificationScript script = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("print 'hello'")
            .build();
        VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network2")
            .withCase(importedCase)
            .withScript(script)
            .build();

        // Update script content
        script.writeScript("print 'bye'");

        // Check that content has been updated in backwards dependencies too
        assertNotNull(virtualCase.getNetwork());
        assertEquals("bye", virtualCase.getOutput());

        // Delete the virtual case
        virtualCase.delete();

        // Check that the dependencies do not have anymore backward dependencies
        assertTrue(importedCase.getBackwardDependencies().isEmpty());
        assertTrue(script.getBackwardDependencies().isEmpty());
    }

    @Test
    void buildingVirtualCaseWithScriptExceptionTest() {
        // test script error
        ModificationScript scriptWithError = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("scriptWithError")
            .withType(ScriptType.GROOVY)
            .withContent("prin 'hello'")
            .build();

        VirtualCase virtualCaseWithError = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network2")
            .withCase(importedCase)
            .withScript(scriptWithError)
            .build();

        ScriptException exception = assertThrows(ScriptException.class, virtualCaseWithError::getNetwork);
        assertNotNull(exception.getError());
        assertTrue(exception.getMessage().contains("No signature of method: test.prin() is applicable"));
    }

    @Test
    void buildingVirtualCaseWithMultipleCompilationErrorsExceptionTest() {
        // test script error
        ModificationScript scriptWithError = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("scriptWithError_MultipleCompilationErrorsException")
            .withType(ScriptType.GROOVY)
            .withContent("print('hello'")
            .build();

        VirtualCase virtualCaseWithError = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network2_MultipleCompilationErrorsException")
            .withCase(importedCase)
            .withScript(scriptWithError)
            .build();

        ScriptException exception = assertThrows(ScriptException.class, virtualCaseWithError::getNetwork);
        assertNotNull(exception.getError());
        assertEquals("Unexpected input: '(' @ line 1, column 6.", exception.getError().getMessage());
    }

    @Test
    void missingDependenciesTest() {
        ModificationScript script = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("print 'hello'")
            .build();

        VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network3")
            .withCase(importedCase)
            .withScript(script)
            .build();

        // Delete the imported case dependency
        importedCase.delete();
        assertTrue(virtualCase.mandatoryDependenciesAreMissing());

        // Create a new imported case and set it as a dependency of the virtual case
        ImportedCase newImportedCase = folder.fileBuilder(ImportedCaseBuilder.class)
            .withCase(aCase)
            .build();
        virtualCase.setCase(newImportedCase);

        // Delete the script dependency
        script.delete();
        assertTrue(virtualCase.mandatoryDependenciesAreMissing());
    }

    @Test
    void replaceDependenciesTest() {
        ModificationScript script = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("print 'hello'")
            .build();

        VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network3")
            .withCase(importedCase)
            .withScript(script)
            .build();

        //test replace dependencies
        assertEquals(importedCase.getName(), virtualCase.getCase().map(ProjectFile::getName).orElse(null));

        // Replace the dependency
        ImportedCase newImportedCase = folder.fileBuilder(ImportedCaseBuilder.class)
            .withCase(aCase)
            .withName("newImportedCase")
            .build();
        virtualCase.replaceDependency(importedCase.getId(), newImportedCase);

        assertNotEquals(importedCase.getName(), virtualCase.getCase().map(ProjectFile::getName).orElse(null));
        assertEquals(newImportedCase.getName(), virtualCase.getCase().map(ProjectFile::getName).orElse(null));
    }

    @Test
    void circularDependencyExceptionTest() {
        ModificationScript script = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("script")
            .withType(ScriptType.GROOVY)
            .withContent("print 'hello'")
            .build();
        VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network3")
            .withCase(importedCase)
            .withScript(script)
            .build();

        // Include the virtual case in itself
        AfsCircularDependencyException exception = assertThrows(AfsCircularDependencyException.class, () -> virtualCase.setCase(virtualCase));
        assertEquals("Circular dependency detected", exception.getMessage());
    }

    @Test
    void networkListenerTest() {
        // test network listener
        ModificationScript scriptModif = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("scriptModif")
            .withType(ScriptType.GROOVY)
            .withContent("network.getSubstation('s1').setTso('tso_new')")
            .build();
        VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network4")
            .withCase(importedCase)
            .withScript(scriptModif)
            .build();

        NetworkListener mockedListener = mock(DefaultNetworkListener.class);
        virtualCase.getNetwork(Collections.singletonList(mockedListener));
        verify(mockedListener, times(1))
            .onUpdate(network.getSubstation("s1"), "tso", null, "TSO", "tso_new");
    }
}
