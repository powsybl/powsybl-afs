/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage;

import com.powsybl.afs.Project;
import com.powsybl.afs.ProjectFile;
import com.powsybl.afs.TaskListener;
import com.powsybl.afs.TaskMonitor;
import com.powsybl.afs.ws.client.utils.ClientUtils;
import com.powsybl.afs.ws.client.utils.UncheckedDeploymentException;
import com.powsybl.afs.ws.utils.AfsRestApi;
import com.powsybl.afs.ws.utils.JsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.websocket.ContainerProvider;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.powsybl.afs.ws.client.utils.ClientUtils.checkOk;
import static com.powsybl.afs.ws.client.utils.ClientUtils.readEntityIfOk;
import static com.powsybl.afs.ws.storage.RemoteAppStorage.getWebTarget;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class RemoteTaskMonitor implements TaskMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteTaskMonitor.class);
    public static final String FILE_SYSTEM_NAME = "fileSystemName";
    private static final String TASK_ID = "taskId";
    private static final String TASK_PATH = "fileSystems/{fileSystemName}/tasks";

    private final String fileSystemName;

    private final URI restUri;

    private final String token;

    private final Map<TaskListener, Session> sessions = new HashMap<>();

    private final Client client;

    private final WebTarget webTarget;

    public RemoteTaskMonitor(String fileSystemName, URI restUri, String token) {
        this.fileSystemName = Objects.requireNonNull(fileSystemName);
        this.restUri = Objects.requireNonNull(restUri);
        this.token = token;

        client = ClientUtils.createClient()
                .register(new JsonProvider());

        webTarget = getWebTarget(client, restUri);
    }

    @Override
    public Task startTask(ProjectFile projectFile) {
        Objects.requireNonNull(projectFile);

        LOGGER.debug("startTask(fileSystemName={}, projectFile={})", fileSystemName, projectFile.getId());

        try (Response response = webTarget.path(TASK_PATH)
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .queryParam("projectFileId", projectFile.getId())
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .put(Entity.text(""))) {
            return readEntityIfOk(response, Task.class);
        }
    }

    @Override
    public Task startTask(String name, Project project) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(project);

        LOGGER.debug("startTask(fileSystemName={}, name={}, project={})", fileSystemName, name, project.getId());

        try (Response response = webTarget.path(TASK_PATH)
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .queryParam("name", name)
            .queryParam("projectId", project.getId())
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .put(Entity.text(""))) {
            return readEntityIfOk(response, Task.class);
        }
    }

    @Override
    public void stopTask(UUID id) {
        LOGGER.debug("stopTask(fileSystemName={}, id={})", fileSystemName, id);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/tasks/{taskId}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(TASK_ID, id)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .delete()) {
            checkOk(response);
        }
    }

    @Override
    public void updateTaskMessage(UUID id, String message) {
        LOGGER.debug("updateTaskMessage(fileSystemName={}, id={})", fileSystemName, id);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/tasks/{taskId}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(TASK_ID, id)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .post(Entity.text(message))) {
            checkOk(response);
        }
    }

    @Override
    public Snapshot takeSnapshot(String projectId) {
        LOGGER.debug("takeSnapshot(fileSystemName={}, projectId={})", fileSystemName, projectId);

        try (Response response = webTarget.path(TASK_PATH)
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .queryParam("projectId", projectId)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, Snapshot.class);
        }
    }

    @Override
    public boolean cancelTaskComputation(UUID id) {
        LOGGER.debug("cancel(fileSystemName={}, id={})", fileSystemName, id);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/tasks/{taskId}/_cancel")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(TASK_ID, id)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .put(null)) {
            return readEntityIfOk(response, Boolean.class);
        }
    }

    @Override
    public void addListener(TaskListener listener) {
        Objects.requireNonNull(listener);

        URI wsUri = SocketsUtils.getWebSocketUri(restUri);
        URI endPointUri = URI.create(wsUri + "/messages/" + AfsRestApi.RESOURCE_ROOT + "/" +
                AfsRestApi.VERSION + "/task_events/" + fileSystemName + "/" + listener.getProjectId());

        LOGGER.debug("Connecting to task event websocket for file system {} at {}", fileSystemName, endPointUri);

        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        try {
            Session session = container.connectToServer(new TaskEventClient(listener), endPointUri);
            sessions.put(listener, session);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DeploymentException e) {
            throw new UncheckedDeploymentException(e);
        }
    }

    @Override
    public void removeListener(TaskListener listener) {
        Objects.requireNonNull(listener);

        Session session = sessions.remove(listener);
        if (session != null) {
            try {
                session.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void updateTaskFuture(UUID taskId, Future<?> future) throws NotACancellableTaskMonitor {
        throw new NotACancellableTaskMonitor("Cannot update task future from remote");
    }

    @Override
    public void close() {
        for (Session session : sessions.values()) {
            try {
                session.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        client.close();
    }
}
