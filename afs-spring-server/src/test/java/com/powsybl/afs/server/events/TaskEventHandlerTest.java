/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.afs.server.events;

import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.TaskEvent;
import com.powsybl.afs.TaskListener;
import com.powsybl.afs.TaskMonitor;
import com.powsybl.afs.server.AppDataWrapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Java6Assertions.assertThat;

/**
 *
 * @author THIYAGARASA Pratheep Ext
 */
@ExtendWith(MockitoExtension.class)
class TaskEventHandlerTest {

    @Mock
    private AppDataWrapper appDataWrapper;

    @Mock
    private WebSocketContext webSocketContext;

    @Mock
    private WebSocketSession socketSession;

    @Mock
    private AppFileSystem appFileSystem;

    @Mock
    private TaskMonitor taskMonitor;

    @Captor
    private ArgumentCaptor<TaskListener> taskListenerArgumentCaptor;

    @Test
    void afterConnection() throws Exception {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("fileSystemName", "fs");
        attributes.put("projectId", "id");
        TaskEventHandler taskEventHandler = new TaskEventHandler(appDataWrapper, webSocketContext);
        Mockito.when(socketSession.getAttributes()).thenReturn(attributes);
        Mockito.when(appDataWrapper.getFileSystem("fs")).thenReturn(appFileSystem);
        Mockito.when(appFileSystem.getTaskMonitor()).thenReturn(taskMonitor);
        taskEventHandler.afterConnectionEstablished(socketSession);

        Mockito.verify(webSocketContext).addSession(socketSession);
        Mockito.verify(taskMonitor).addListener(taskListenerArgumentCaptor.capture());
        assertThat(taskListenerArgumentCaptor.getValue().getProjectId()).isEqualTo("id");
    }

    @Test
    void afterConnectionClose() throws Exception {
        TaskListener listener = new TaskListener() {
            @Override
            public String getProjectId() {
                return "id";
            }

            @Override
            public void onEvent(TaskEvent event) {
            }
        };

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("fileSystemName", "fs");
        attributes.put("taskListener", listener);
        TaskEventHandler taskEventHandler = new TaskEventHandler(appDataWrapper, webSocketContext);
        Mockito.when(socketSession.getAttributes()).thenReturn(attributes);
        Mockito.when(appDataWrapper.getFileSystem("fs")).thenReturn(appFileSystem);
        Mockito.when(appFileSystem.getTaskMonitor()).thenReturn(taskMonitor);
        taskEventHandler.afterConnectionClosed(socketSession, CloseStatus.NORMAL);

        Mockito.verify(webSocketContext).removeSession(socketSession);
        Mockito.verify(taskMonitor).removeListener(Mockito.eq(listener));
    }
}
