/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.testing.EqualsTester;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.InMemoryEventsBus;
import com.powsybl.commons.json.JsonUtil;
import com.powsybl.computation.CompletableFutureTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class LocalTaskMonitorTest extends AbstractProjectFileTest {

    private Project test;
    private FooFile foo;
    private Deque<TaskEvent> events;
    private TaskListener listener;

    @Override
    @BeforeEach
    public void setup() throws IOException {
        super.setup();

        test = afs.getRootFolder().createProject("test");
        foo = test.getRootFolder().fileBuilder(FooFileBuilder.class)
            .withName("foo")
            .build();
        events = new ArrayDeque<>();
        listener = new TaskListener() {
            @Override
            public String getProjectId() {
                return test.getId();
            }

            @Override
            public void onEvent(TaskEvent event) {
                events.add(event);
            }
        };
    }

    @Override
    protected List<ProjectFileExtension> getProjectFileExtensions() {
        return Collections.singletonList(new FooFileExtension());
    }

    @Override
    protected AppStorage createStorage() {
        return MapDbAppStorage.createMem("mem", new InMemoryEventsBus());
    }

    @Test
    void legacyTaskTest() {
        TaskMonitor.Task legacyTask = new TaskMonitor.Task("test", "test", 1L, test.getId());
        assertNull(legacyTask.getNodeId());
    }

    @Test
    void snapshotOnNullTest() {
        try (TaskMonitor monitor = new LocalTaskMonitor()) {
            assertEquals(0L, monitor.takeSnapshot(null).getRevision());
            assertTrue(monitor.takeSnapshot(null).getTasks().isEmpty());
        }
    }

    @Test
    void startTaskTest() {
        try (TaskMonitor monitor = new LocalTaskMonitor()) {
            // Add a listener and start a task
            monitor.addListener(listener);
            TaskMonitor.Task task = monitor.startTask(foo);

            // Checks on the task
            assertEquals("foo", task.getName());
            assertEquals(foo.getId(), task.getNodeId());

            // Checks on the listener
            assertEquals(1, events.size());
            assertEquals(new StartTaskEvent(task.getId(), 1L, "foo"), events.pop());

            // Checks on the TaskMonitor
            assertEquals(1L, monitor.takeSnapshot(null).getRevision());
            assertEquals(1, monitor.takeSnapshot(null).getTasks().size());
            assertEquals(task.getId(), monitor.takeSnapshot(null).getTasks().get(0).getId());
            assertEquals(1L, monitor.takeSnapshot(null).getTasks().get(0).getRevision());
        }
    }

    @Test
    void serDeSnapshotTest() throws IOException {
        try (TaskMonitor monitor = new LocalTaskMonitor()) {
            // Add a listener and start a task
            monitor.addListener(listener);
            monitor.startTask(foo);

            // test Snapshot -> json -> Snapshot
            ObjectMapper objectMapper = JsonUtil.createObjectMapper();
            TaskMonitor.Snapshot snapshotRef = monitor.takeSnapshot(null);
            String snJsonRef = objectMapper.writeValueAsString(snapshotRef);
            TaskMonitor.Snapshot snapshotConverted = objectMapper.readValue(snJsonRef, TaskMonitor.Snapshot.class);
            assertEquals(snapshotRef, snapshotConverted);
        }
    }

    @Test
    void updateTaskTest() {
        try (TaskMonitor monitor = new LocalTaskMonitor()) {
            // Start a task and add a listener
            TaskMonitor.Task task = monitor.startTask(foo);
            monitor.addListener(listener);

            // Update task
            monitor.updateTaskMessage(task.getId(), "hello");

            // Check on the listener
            assertEquals(1, events.size());
            assertEquals(new UpdateTaskMessageEvent(task.getId(), 2L, "hello"), events.pop());

            // Checks on the TaskMonitor
            assertEquals(2L, monitor.takeSnapshot(null).getRevision());
            assertEquals(1, monitor.takeSnapshot(null).getTasks().size());
            assertEquals(task.getId(), monitor.takeSnapshot(null).getTasks().get(0).getId());
            assertEquals("hello", monitor.takeSnapshot(null).getTasks().get(0).getMessage());
            assertEquals(2L, monitor.takeSnapshot(null).getTasks().get(0).getRevision());
        }
    }

    @Test
    void updateTaskMessageExceptionTest() {
        try (TaskMonitor monitor = new LocalTaskMonitor()) {
            UUID uuid = new UUID(0L, 0L);
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> monitor.updateTaskMessage(uuid, ""));
            assertEquals("Task '00000000-0000-0000-0000-000000000000' not found", exception.getMessage());
        }
    }

    @Test
    void taskInterruptionTest() throws TaskMonitor.NotACancellableTaskMonitor, InterruptedException {
        try (TaskMonitor monitor = new LocalTaskMonitor()) {
            // Start a task and add a listener
            TaskMonitor.Task task = monitor.startTask(foo);
            monitor.addListener(listener);

            // Update the task
            monitor.updateTaskFuture(task.getId(), null);

            // Check on the listener
            assertEquals(1, events.size());
            assertEquals(new TaskCancellableStatusChangeEvent(task.getId(), 2L, false), events.pop());

            CountDownLatch waitForStart = new CountDownLatch(1);
            CountDownLatch waitIndefinitely = new CountDownLatch(1);
            CountDownLatch waitForInterruption = new CountDownLatch(1);

            AtomicBoolean interrupted = new AtomicBoolean(false);
            CompletableFutureTask<Void> dummyTaskProcess = CompletableFutureTask.runAsync(() -> {
                waitForStart.countDown();
                try {
                    waitIndefinitely.await();
                    fail();
                } catch (InterruptedException exc) {
                    interrupted.set(true);
                    waitForInterruption.countDown();
                }
                return null;
            }, Executors.newSingleThreadExecutor());

            // Start the task
            waitForStart.await();
            monitor.updateTaskFuture(task.getId(), dummyTaskProcess);

            // Checks on the listener
            assertEquals(1, events.size());
            assertEquals(new TaskCancellableStatusChangeEvent(task.getId(), 3L, true), events.pop());

            // Cancel the task
            assertThat(monitor.cancelTaskComputation(task.getId())).isTrue();
            assertThat(dummyTaskProcess.isCancelled()).isTrue();

            assertThrows(CancellationException.class, dummyTaskProcess::get);
            waitForInterruption.await();

            // Check that the task has really been interrupted
            assertThat(waitForInterruption.getCount()).isZero();
            assertThat(interrupted.get()).isTrue();
            assertThat(waitIndefinitely.getCount()).isEqualTo(1);
        }
    }

    @Test
    void stopTaskTest() {
        try (TaskMonitor monitor = new LocalTaskMonitor()) {
            // Start a task and add a listener
            TaskMonitor.Task task = monitor.startTask(foo);
            monitor.updateTaskMessage(task.getId(), "hello");
            monitor.addListener(listener);

            // Stop the task
            monitor.stopTask(task.getId());

            // Checks on the listener
            assertEquals(1, events.size());
            assertEquals(new StopTaskEvent(task.getId(), 3L), events.pop());

            assertEquals(3L, monitor.takeSnapshot(null).getRevision());
            assertTrue(monitor.takeSnapshot(null).getTasks().isEmpty());
        }
    }

    @Test
    void stopTaskExceptionTest() {
        try (TaskMonitor monitor = new LocalTaskMonitor()) {
            UUID uuid = new UUID(0L, 0L);
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> monitor.stopTask(uuid));
            assertEquals("Task '00000000-0000-0000-0000-000000000000' not found", exception.getMessage());
        }
    }

    @Test
    void startTaskEventTest() throws IOException {
        TaskEvent event = new StartTaskEvent(new UUID(0L, 0L), 0L, "e1");
        assertEquals("StartTaskEvent(taskId=00000000-0000-0000-0000-000000000000, revision=0, nodeId=null, name=e1)", event.toString());
        ObjectMapper objectMapper = JsonUtil.createObjectMapper();
        String json = objectMapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(event);
        String jsonRef = String.join(System.lineSeparator(),
            "{",
            "  \"@c\" : \".StartTaskEvent\",",
            "  \"taskId\" : \"00000000-0000-0000-0000-000000000000\",",
            "  \"revision\" : 0,",
            "  \"name\" : \"e1\",",
            "  \"nodeId\" : null",
            "}");
        assertEquals(jsonRef, json);
        TaskEvent event2 = objectMapper.readValue(json, TaskEvent.class);
        assertEquals(event, event2);

        new EqualsTester()
            .addEqualityGroup(new StartTaskEvent(new UUID(0L, 0L), 0L, "e1"), new StartTaskEvent(new UUID(0L, 0L), 0L, "e1"))
            .addEqualityGroup(new StartTaskEvent(new UUID(0L, 1L), 1L, "e2"), new StartTaskEvent(new UUID(0L, 1L), 1L, "e2"))
            .testEquals();
    }

    @Test
    void stopTaskEventTest() throws IOException {
        TaskEvent event = new StopTaskEvent(new UUID(0L, 1L), 1L);
        assertEquals("StopTaskEvent(taskId=00000000-0000-0000-0000-000000000001, revision=1)", event.toString());
        ObjectMapper objectMapper = JsonUtil.createObjectMapper();
        String json = objectMapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(event);
        String jsonRef = String.join(System.lineSeparator(),
            "{",
            "  \"@c\" : \".StopTaskEvent\",",
            "  \"taskId\" : \"00000000-0000-0000-0000-000000000001\",",
            "  \"revision\" : 1",
            "}");
        assertEquals(jsonRef, json);
        TaskEvent event2 = objectMapper.readValue(json, TaskEvent.class);
        assertEquals(event, event2);

        new EqualsTester()
            .addEqualityGroup(new StopTaskEvent(new UUID(0L, 0L), 0L), new StopTaskEvent(new UUID(0L, 0L), 0L))
            .addEqualityGroup(new StopTaskEvent(new UUID(0L, 1L), 1L), new StopTaskEvent(new UUID(0L, 1L), 1L))
            .testEquals();
    }

    @Test
    void updateTaskMessageEventTest() throws IOException {
        TaskEvent event = new UpdateTaskMessageEvent(new UUID(0L, 2L), 2L, "hello");
        assertEquals("UpdateTaskMessageEvent(taskId=00000000-0000-0000-0000-000000000002, revision=2, message=hello)", event.toString());
        ObjectMapper objectMapper = JsonUtil.createObjectMapper();
        String json = objectMapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(event);
        String jsonRef = String.join(System.lineSeparator(),
            "{",
            "  \"@c\" : \".UpdateTaskMessageEvent\",",
            "  \"taskId\" : \"00000000-0000-0000-0000-000000000002\",",
            "  \"revision\" : 2,",
            "  \"message\" : \"hello\"",
            "}");
        assertEquals(jsonRef, json);
        TaskEvent event2 = objectMapper.readValue(json, TaskEvent.class);
        assertEquals(event, event2);

        new EqualsTester()
            .addEqualityGroup(new UpdateTaskMessageEvent(new UUID(0L, 0L), 0L, "hello"), new UpdateTaskMessageEvent(new UUID(0L, 0L), 0L, "hello"))
            .addEqualityGroup(new UpdateTaskMessageEvent(new UUID(0L, 1L), 1L, "bye"), new UpdateTaskMessageEvent(new UUID(0L, 1L), 1L, "bye"))
            .testEquals();
    }

    @Test
    void updateTaskFutureTest() {
        try (TaskMonitor monitor = new LocalTaskMonitor()) {
            UUID uuid = new UUID(0L, 0L);
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> monitor.updateTaskFuture(uuid, null));
            assertEquals("Task '00000000-0000-0000-0000-000000000000' not found", exception.getMessage());
        }
    }
}
