/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class LocalTaskMonitor implements TaskMonitor {

    private static final String TASK_NOT_FOUND = "Task '%s' not found";
    private final Map<UUID, Task> tasks = new HashMap<>();
    private final Map<UUID, Future> tasksFuture = new HashMap<>();
    private final Lock lock = new ReentrantLock();
    private final List<TaskListener> listeners = new ArrayList<>();
    private long revision = 0L;

    @Override
    public Task startTask(ProjectFile projectFile) {
        Objects.requireNonNull(projectFile);

        return startTask(projectFile.getName(), projectFile.getProject(), projectFile.getId());
    }

    @Override
    public Task startTask(String name, Project project) {
        return startTask(name, project, null);
    }

    public Task startTask(String name, Project project, String nodeId) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(project);

        lock.lock();
        try {
            revision++;
            Task task = new Task(name, null, revision, project.getId(), nodeId);
            tasks.put(task.getId(), task);

            // notification
            notifyListeners(new StartTaskEvent(task.getId(), revision, name, nodeId), task.getProjectId());

            return task;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stopTask(UUID id) {
        Objects.requireNonNull(id);
        lock.lock();
        try {
            Task task = tasks.remove(id);
            if (task == null) {
                throw new IllegalArgumentException(String.format(TASK_NOT_FOUND, id));
            }
            revision++;

            // remove optional related future
            tasksFuture.remove(id);

            // notification
            notifyListeners(new StopTaskEvent(id, revision), task.getProjectId());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Snapshot takeSnapshot(String projectId) {
        lock.lock();
        try {
            return new Snapshot(tasks.values().stream()
                .filter(task -> projectId == null || task.getProjectId().equals(projectId))
                .map(Task::new).collect(Collectors.toList()),
                revision);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean cancelTaskComputation(UUID id) {
        if (tasksFuture.containsKey(id)) {
            return tasksFuture.get(id).cancel(true);
        }
        return false;
    }

    @Override
    public void updateTaskMessage(UUID id, String message) {
        Objects.requireNonNull(id);
        lock.lock();
        try {
            Task task = tasks.get(id);
            if (task == null) {
                throw new IllegalArgumentException(String.format(TASK_NOT_FOUND, id));
            }
            revision++;
            task.setMessage(message);
            task.setRevision(revision);

            // notification
            notifyListeners(new UpdateTaskMessageEvent(id, revision, message), task.getProjectId());
        } finally {
            lock.unlock();
        }
    }

    private void notifyListeners(TaskEvent event, String projectId) {
        for (TaskListener listener : listeners) {
            if (listener.getProjectId() == null || listener.getProjectId().equals(projectId)) {
                listener.onEvent(event);
            }
        }
    }

    @Override
    public void addListener(TaskListener listener) {
        Objects.requireNonNull(listener);
        lock.lock();
        try {
            listeners.add(listener);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void removeListener(TaskListener listener) {
        Objects.requireNonNull(listener);
        lock.lock();
        try {
            listeners.remove(listener);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void updateTaskFuture(UUID taskId, Future<?> future) {
        Objects.requireNonNull(taskId);
        lock.lock();
        try {
            Task task = tasks.get(taskId);
            if (task == null) {
                throw new IllegalArgumentException(String.format(TASK_NOT_FOUND, taskId));
            }
            tasksFuture.put(taskId, future);
            task.setCancellable(future != null);
            revision++;
            task.setRevision(revision);

            // notification
            notifyListeners(new TaskCancellableStatusChangeEvent(taskId, revision, task.isCancellable()), task.getProjectId());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        // nothing to clean
    }
}
