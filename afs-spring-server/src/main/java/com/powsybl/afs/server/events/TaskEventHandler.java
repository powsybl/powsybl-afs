/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.TaskEvent;
import com.powsybl.afs.TaskListener;
import com.powsybl.afs.ws.server.utils.AppDataBean;
import com.powsybl.commons.json.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.io.UncheckedIOException;

public class TaskEventHandler extends TextWebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskEventHandler.class);

    private final AppDataBean appDataBean;
    private final WebSocketContext webSocketContext;

    private final ObjectMapper objectMapper = JsonUtil.createObjectMapper();

    public TaskEventHandler(AppDataBean appDataBean, WebSocketContext webSocketContext) {
        this.appDataBean = appDataBean;
        this.webSocketContext = webSocketContext;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {

        String fileSystemName = SessionAttributes.of(session).getFilesystem();
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        String projectId = session.getAttributes().get("projectId").toString();

        LOGGER.debug("WebSocket session '{}' opened for file system '{}'", session.getId(), fileSystemName);

        TaskListener listener = new TaskListener() {

            private final WebSocketSession internalSession =  WebSocketUtils.concurrent(session);

            @Override
            public String getProjectId() {
                return projectId;
            }

            @Override
            public void onEvent(TaskEvent event) {
                if (internalSession.isOpen()) {
                    try {
                        String taskEventEncode = objectMapper.writeValueAsString(event);
                        internalSession.sendMessage(new TextMessage(taskEventEncode));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }
        };

        SessionAttributes.of(session).setTaskListener(listener);
        fileSystem.getTaskMonitor().addListener(listener);

        webSocketContext.addSession(session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        removeSession(session);
    }

    private void removeSession(WebSocketSession session) {
        SessionAttributes attrs = SessionAttributes.of(session);
        AppFileSystem fileSystem = appDataBean.getFileSystem(attrs.getFilesystem());

        fileSystem.getTaskMonitor().removeListener(attrs.getTaskListener());
        webSocketContext.removeSession(session);
    }
}
