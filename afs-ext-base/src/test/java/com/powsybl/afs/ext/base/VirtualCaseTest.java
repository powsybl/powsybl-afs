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
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.iidm.network.ImportConfig;
import com.powsybl.iidm.network.ImportersLoader;
import com.powsybl.iidm.network.ImportersLoaderList;
import com.powsybl.iidm.network.DefaultNetworkListener;
import com.powsybl.iidm.network.NetworkListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class VirtualCaseTest extends AbstractProjectFileTest {

    private ImportersLoader createImportersLoader() {
        return new ImportersLoaderList(new TestImporter(network));
    }

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    @Override
    protected List<FileExtension> getFileExtensions() {
        return ImmutableList.of(new CaseExtension(createImportersLoader()));
    }

    @Override
    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return ImmutableList.of(new ImportedCaseExtension(createImportersLoader(), new ImportConfig()),
                                new ModificationScriptExtension(),
                                new VirtualCaseExtension());
    }

    @Override
    protected List<ServiceExtension> getServiceExtensions() {
        return ImmutableList.of(new LocalNetworkCacheServiceExtension());
    }

    @BeforeEach
    public void setup() throws IOException {
        super.setup();
        NodeInfo rootFolderInfo = storage.createRootNodeIfNotExists("root", Folder.PSEUDO_CLASS);
        NodeInfo nodeInfo = storage.createNode(rootFolderInfo.getId(), "network", Case.PSEUDO_CLASS, "", Case.VERSION,
                new NodeGenericMetadata().setString(Case.FORMAT, TestImporter.FORMAT));
        storage.setConsistent(nodeInfo.getId());
    }

    @Test
    void test() {
        // get case
        Case aCase = (Case) afs.getRootFolder().getChildren().get(0);

        // create project
        Project project = afs.getRootFolder().createProject("project");

        // create project folder
        ProjectFolder folder = project.getRootFolder().createFolder("folder");

        // import case into project
        ImportedCase importedCase = folder.fileBuilder(ImportedCaseBuilder.class)
                .withCase(aCase)
                .build();

        // create groovy script
        ModificationScript script = folder.fileBuilder(ModificationScriptBuilder.class)
                .withName("script")
                .withType(ScriptType.GROOVY)
                .withContent("print 'hello'")
                .build();

        // create virtual by applying groovy script on imported case
        try {
            VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
                    .withCase(importedCase)
                    .withScript(script)
                    .build();
            fail();
        } catch (AfsException ignored) {
        }

        try {
            VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
                    .withName("network2")
                    .withScript(script)
                    .build();
            fail();
        } catch (AfsException ignored) {
        }

        try {
            VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
                    .withName("network2")
                    .withCase(importedCase)
                    .build();
            fail();
        } catch (AfsException ignored) {
        }

        VirtualCase virtualCase = folder.fileBuilder(VirtualCaseBuilder.class)
                .withName("network2")
                .withCase(importedCase)
                .withScript(script)
                .build();

        assertEquals("network2", virtualCase.getName());
        assertTrue(virtualCase.getCase().isPresent());
        assertTrue(virtualCase.getScript().isPresent());
        assertEquals(2, virtualCase.getDependencies().size());
        assertEquals(1, importedCase.getBackwardDependencies().size());
        assertEquals(1, script.getBackwardDependencies().size());
        assertNotNull(virtualCase.getNetwork());
        assertFalse(virtualCase.mandatoryDependenciesAreMissing());
        assertEquals("hello", virtualCase.getOutput());

        // test cache invalidation
        script.writeScript("print 'bye'");
        assertNotNull(virtualCase.getNetwork());
        assertEquals("bye", virtualCase.getOutput());

        virtualCase.delete();
        assertTrue(importedCase.getBackwardDependencies().isEmpty());
        assertTrue(script.getBackwardDependencies().isEmpty());

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

        try {
            virtualCaseWithError.getNetwork();
            fail();
        } catch (ScriptException e) {
            assertNotNull(e.getError());
            assertTrue(e.getError().getMessage().contains("No signature of method: test.prin() is applicable"));
        }

        // test script error with MultipleCompilationErrorsException
        scriptWithError = folder.fileBuilder(ModificationScriptBuilder.class)
            .withName("scriptWithError_MultipleCompilationErrorsException")
            .withType(ScriptType.GROOVY)
            .withContent("print('hello'")
            .build();

        virtualCaseWithError = folder.fileBuilder(VirtualCaseBuilder.class)
            .withName("network2_MultipleCompilationErrorsException")
            .withCase(importedCase)
            .withScript(scriptWithError)
            .build();

        try {
            virtualCaseWithError.getNetwork();
            fail();
        } catch (ScriptException e) {
            assertNotNull(e.getError());
            assertEquals("Unexpected input: '(' @ line 1, column 6.", e.getError().getMessage());
        }

        //test missing dependencies
        VirtualCase virtualCase3 = folder.fileBuilder(VirtualCaseBuilder.class)
                .withName("network3")
                .withCase(importedCase)
                .withScript(scriptWithError)
                .build();

        importedCase.delete();
        assertTrue(virtualCase3.mandatoryDependenciesAreMissing());

        ImportedCase importedCase2 = folder.fileBuilder(ImportedCaseBuilder.class)
                .withCase(aCase)
                .build();

        virtualCase3.setCase(importedCase2);

        scriptWithError.delete();
        assertTrue(virtualCase3.mandatoryDependenciesAreMissing());

        //test replace dependencies
        assertEquals(importedCase2.getName(), virtualCase3.getCase().map(ProjectFile::getName).orElse(null));

        ImportedCase importedCase3 = folder.fileBuilder(ImportedCaseBuilder.class)
                .withCase(aCase)
                .withName("importedCase3")
                .build();

        virtualCase3.replaceDependency(importedCase2.getId(), importedCase3);

        assertNotEquals(importedCase2.getName(), virtualCase3.getCase().map(ProjectFile::getName).orElse(null));
        assertEquals(importedCase3.getName(), virtualCase3.getCase().map(ProjectFile::getName).orElse(null));

        assertThatCode(() -> virtualCase3.setCase(virtualCase3)).isInstanceOf(AfsCircularDependencyException.class);

        // test network listener
        ModificationScript scriptModif = folder.fileBuilder(ModificationScriptBuilder.class)
                .withName("scriptModif")
                .withType(ScriptType.GROOVY)
                .withContent("network.getSubstation('s1').setTso('tso_new')")
                .build();
        VirtualCase virtualCase4 = folder.fileBuilder(VirtualCaseBuilder.class)
                .withName("network4")
                .withCase(importedCase3)
                .withScript(scriptModif)
                .build();

        NetworkListener mockedListener = mock(DefaultNetworkListener.class);
        virtualCase4.getNetwork(Collections.singletonList(mockedListener));
        verify(mockedListener, times(1))
                .onUpdate(network.getSubstation("s1"), "tso", null, "TSO", "tso_new");
    }
}
