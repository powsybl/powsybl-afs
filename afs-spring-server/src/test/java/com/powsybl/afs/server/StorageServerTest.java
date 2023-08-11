/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.afs.server;

import com.google.common.collect.ImmutableList;
import com.powsybl.afs.*;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import com.powsybl.afs.storage.check.FileSystemCheckOptionsBuilder;
import com.powsybl.afs.ws.storage.RemoteAppStorage;
import com.powsybl.afs.ws.storage.RemoteTaskMonitor;
import com.powsybl.commons.exceptions.UncheckedUriSyntaxException;
import com.powsybl.computation.ComputationManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.servlet.ServletContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * @author THIYAGARASA Pratheep Ext
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = StorageServer.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EnableAutoConfiguration
@ActiveProfiles("test")
public class StorageServerTest extends AbstractAppStorageTest {

    private static final String FS_TEST_NAME = "test";
    @LocalServerPort
    private int port;

    @Autowired
    private ServletContext servletContext;

    @Autowired
    private AppDataWrapper appDataWrapper;

    @TestConfiguration
    public static class AppDataConfig {
        @Bean
        public AppData getAppData() {
            EventsBus eventBus = new InMemoryEventsBus();
            AppStorage storage = Mockito.spy(MapDbAppStorage.createMem("mem", eventBus));
            AppFileSystem fs = new AppFileSystem(FS_TEST_NAME, true, storage, new LocalTaskMonitor());
            ComputationManager cm = Mockito.mock(ComputationManager.class);

            List<AppFileSystemProvider> fsProviders = ImmutableList.of(m -> ImmutableList.of(fs));
            return new AppData(cm, cm, fsProviders, eventBus);
        }
    }

    private URI getRestUri() {
        try {
            String sheme = "http";
            return new URI(sheme + "://localhost:" + port + servletContext.getContextPath());
        } catch (URISyntaxException e) {
            throw new UncheckedUriSyntaxException(e);
        }
    }

    @Override
    protected AppStorage createStorage() {
        URI restUri = getRestUri();
        return new RemoteAppStorage(FS_TEST_NAME, restUri, "");
    }

    @Override
    protected void nextDependentTests() throws InterruptedException {
        super.nextDependentTests();
        RemoteTaskMonitor taskMonitor = new RemoteTaskMonitor(FS_TEST_NAME, getRestUri(), null);
        NodeInfo root = storage.createRootNodeIfNotExists(storage.getFileSystemName(), Folder.PSEUDO_CLASS);
        NodeInfo projectNode = storage.createNode(root.getId(), "project", Project.PSEUDO_CLASS, "test project", 0, new NodeGenericMetadata());

        Project project = Mockito.mock(Project.class);
        when(project.getId()).thenReturn(projectNode.getId());
        TaskMonitor.Task task = taskMonitor.startTask("task_test", project);
        assertThat(task).isNotNull();
        TaskMonitor.Snapshot snapshot = taskMonitor.takeSnapshot(project.getId());
        assertThat(snapshot.getTasks().stream().anyMatch(t -> t.getId().equals(task.getId()))).isTrue();

        taskMonitor.updateTaskMessage(task.getId(), "new Message");
        TaskMonitor.Snapshot snapshotAfterUpdate = taskMonitor.takeSnapshot(project.getId());
        TaskMonitor.Task taskAfterUpdate = snapshotAfterUpdate.getTasks().stream().filter(t -> t.getId().equals(task.getId())).findFirst().get();
        assertThat(taskAfterUpdate.getId()).isEqualTo(task.getId());
        assertThat(taskAfterUpdate.getMessage()).isEqualTo("new Message");

        taskMonitor.stopTask(task.getId());
        TaskMonitor.Snapshot snapshotAfterStop = taskMonitor.takeSnapshot(project.getId());
        assertThat(snapshotAfterStop.getTasks().stream().anyMatch(t -> t.getId().equals(task.getId()))).isFalse();

        assertThatCode(() -> taskMonitor.updateTaskFuture(task.getId(), CompletableFuture.runAsync(() -> {
        }))).isInstanceOf(TaskMonitor.NotACancellableTaskMonitor.class);
        assertThat(taskMonitor.cancelTaskComputation(task.getId())).isFalse();

        // cleanup
        storage.deleteNode(projectNode.getId());

        // clear events
        eventStack.take();
        eventStack.take();

        testFileSystemCheck();
    }

    private static FileSystemCheckIssue issue(boolean repaired) {
        return new FileSystemCheckIssue()
            .setNodeId("id")
            .setNodeName("toto")
            .setRepaired(repaired)
            .setDescription("my issue")
            .setType("TEST")
            .setResolutionDescription(repaired ? "resolved" : "");
    }

    @Test
    public void testFileSystemCheck() {
        AppStorage backendStorage = appDataWrapper.getStorage(FS_TEST_NAME);

        doReturn(Collections.singletonList("TEST"))
            .when(backendStorage).getSupportedFileSystemChecks();
        doReturn(Collections.singletonList(issue(true)))
            .when(backendStorage).checkFileSystem(argThat(opt -> opt.isRepair() && opt.getTypes().contains("TEST")));
        doReturn(Collections.singletonList(issue(false)))
            .when(backendStorage).checkFileSystem(argThat(opt -> !opt.isRepair() && opt.getTypes().contains("TEST")));

        assertThat(storage.getSupportedFileSystemChecks())
            .containsExactly("TEST");

        final FileSystemCheckOptions dryRunOptions = new FileSystemCheckOptionsBuilder()
            .addCheckTypes("TEST")
            .dryRun()
            .build();
        Assertions.assertThat(storage.checkFileSystem(dryRunOptions))
            .containsExactly(issue(false));

        final FileSystemCheckOptions repairOptions = new FileSystemCheckOptionsBuilder()
            .addCheckTypes("TEST")
            .repair()
            .build();
        Assertions.assertThat(storage.checkFileSystem(repairOptions))
            .containsExactly(issue(true));

        final FileSystemCheckOptions otherFsOptions = new FileSystemCheckOptionsBuilder().build();
        Assertions.assertThat(storage.checkFileSystem(otherFsOptions))
            .isEmpty();
    }
}
