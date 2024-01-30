/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server;

import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.ws.client.utils.ClientUtils;
import com.powsybl.afs.ws.client.utils.UserSession;
import com.powsybl.afs.ws.server.utils.AppDataBean;
import com.powsybl.afs.ws.storage.RemoteAppStorage;
import com.powsybl.commons.exceptions.UncheckedUriSyntaxException;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Ali Tahanout {@literal <ali.tahanout at rte-france.com>}
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@ExtendWith(ArquillianExtension.class)
@RunAsClient
public class AppStorageServerTest extends AbstractAppStorageTest {

    @ArquillianResource
    private URL baseUrl;

    private UserSession userSession;

    @Inject
    private AppDataBean appDataBean;

    @Deployment
    public static WebArchive createTestArchive() {
        File[] filesLib = Maven.configureResolver()
            .useLegacyLocalRepo(true)
            .withMavenCentralRepo(false)
            .withClassPathResolution(true)
            .loadPomFromFile("pom.xml")
            .importRuntimeDependencies()
            .resolve("org.mockito:mockito-core",
                "com.powsybl:powsybl-config-test",
                "com.powsybl:powsybl-afs-ws-server-utils",
                "com.powsybl:powsybl-afs-ws-utils",
                "com.powsybl:powsybl-afs-mapdb")
            .withTransitivity()
            .asFile();

        return ShrinkWrap.create(WebArchive.class, "afs-ws-server-test.war")
            .addAsWebInfResource("META-INF/beans.xml")
            .addPackage(AppStorageServerTest.class.getPackage())
            .addAsLibraries(filesLib);
    }

    private URI getRestUri() {
        try {
            return baseUrl.toURI();
        } catch (URISyntaxException e) {
            throw new UncheckedUriSyntaxException(e);
        }
    }

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        userSession = ClientUtils.authenticate(getRestUri(), "", "");
        super.setUp();
    }

    @Override
    protected AppStorage createStorage() {
        URI restUri = getRestUri();
        RemoteAppStorage storage = new RemoteAppStorage(AppDataBeanMock.TEST_FS_NAME, restUri,
            userSession.getToken());
        return storage;
    }

    @Disabled("waiting for a decision on module removal")
    @Test
    void getFileSystemNamesTest() {
        List<String> fileSystemNames = RemoteAppStorage.getFileSystemNames(getRestUri(), userSession.getToken());
        assertEquals(Collections.singletonList(AppDataBeanMock.TEST_FS_NAME), fileSystemNames);
    }

    @Disabled("waiting for a decision on module removal")
    @Test
    @Override
    public void test() {
        // Test temporary commented - waiting for a decision on module removal
    }

    @Override
    protected void nextDependentTests() throws InterruptedException {
        super.nextDependentTests();

        // Test temporary commented - waiting for a decision on module removal

//        RemoteTaskMonitor taskMonitor = new RemoteTaskMonitor(AppDataBeanMock.TEST_FS_NAME, getRestUri(), userSession.getToken());
//        NodeInfo root = storage.createRootNodeIfNotExists(storage.getFileSystemName(), Folder.PSEUDO_CLASS);
//        NodeInfo projectNode = storage.createNode(root.getId(), "project", Project.PSEUDO_CLASS, "test project", 0, new NodeGenericMetadata());
//
//        Project project = Mockito.mock(Project.class);
//        when(project.getId()).thenReturn(projectNode.getId());
//        TaskMonitor.Task task = taskMonitor.startTask("task_test", project);
//        assertNotNull(task);
//        TaskMonitor.Snapshot snapshot = taskMonitor.takeSnapshot(project.getId());
//        assertTrue(snapshot.getTasks().stream().anyMatch(t -> t.getId().equals(task.getId())));
//
//        assertThatCode(() -> taskMonitor.updateTaskFuture(task.getId(), CompletableFuture.runAsync(() -> {
//        }))).isInstanceOf(TaskMonitor.NotACancellableTaskMonitor.class);
//        assertFalse(taskMonitor.cancelTaskComputation(task.getId()));
//
//        // cleanup
//        storage.deleteNode(projectNode.getId());
//
//        // clear events
//        eventStack.take();
//        eventStack.take();

    }

    @Disabled("waiting for a decision on module removal")
    @Test
    void handleRegisteredErrorTest() {
        assertThatCode(() -> ClientUtils.checkOk(ClientUtils.createClient().target(getRestUri()).path("/rest/dummy/registeredError").request().get()))
            .isInstanceOf(CancellationException.class);
        assertThatCode(() -> ClientUtils.checkOk(ClientUtils.createClient().target(getRestUri()).path("/rest/dummy/unregisteredError").request().get()))
            .isInstanceOf(AfsStorageException.class);
        assertThatCode(() -> ClientUtils.checkOk(ClientUtils.createClient().target(getRestUri()).path("/rest/dummy/registeredErrorWithMessage").request().get()))
            .isInstanceOf(NotImplementedException.class).hasMessage("hello");
    }

}
