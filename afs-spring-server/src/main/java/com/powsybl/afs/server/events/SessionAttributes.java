/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.events;

import com.powsybl.afs.TaskListener;
import com.powsybl.afs.storage.events.AppStorageListener;
import org.springframework.web.socket.WebSocketSession;

import java.util.Map;

/**
 * Utility to get/set session attributes.
 *
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class SessionAttributes {

    private static final String LISTENER_ATTRIBUTE = "listener";
    private static final String TASK_LISTENER_ATTRIBUTE = "taskListener";
    private static final String FILESYSTEM_ATTRIBUTE = "fileSystemName";

    private final Map<String, Object> attributes;

    public SessionAttributes(WebSocketSession session) {
        this.attributes = session.getAttributes();
    }

    public static SessionAttributes of(WebSocketSession session) {
        return new SessionAttributes(session);
    }

    public void setListener(AppStorageListener listener) {
        attributes.put(LISTENER_ATTRIBUTE, listener);
    }

    public AppStorageListener getListener() {
        return (AppStorageListener) attributes.get(LISTENER_ATTRIBUTE);
    }

    public void setTaskListener(TaskListener listener) {
        attributes.put(TASK_LISTENER_ATTRIBUTE, listener);
    }

    public TaskListener getTaskListener() {
        return (TaskListener) attributes.get(TASK_LISTENER_ATTRIBUTE);
    }

    public String getFilesystem() {
        return (String) attributes.get(FILESYSTEM_ATTRIBUTE);
    }
}
