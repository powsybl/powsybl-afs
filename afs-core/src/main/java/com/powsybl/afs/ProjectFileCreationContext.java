/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeInfo;

import java.util.Objects;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class ProjectFileCreationContext extends ProjectFileContext {

    private final boolean connected;

    private final NodeInfo info;

    public ProjectFileCreationContext(NodeInfo info, AppStorage storage, Project project) {
        this(info, storage, project, true);
    }

    public ProjectFileCreationContext(NodeInfo info, AppStorage storage, Project project, boolean connected) {
        super(storage, project);
        this.info = Objects.requireNonNull(info);
        this.connected = connected;
    }

    public NodeInfo getInfo() {
        return info;
    }

    public boolean isConnected() {
        return connected;
    }
}
