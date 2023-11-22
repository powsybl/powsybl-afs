/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.NetworkListener;

import java.util.List;

/**
 * Common interface for project files able to provide a Network.
 *
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public interface ProjectCase {

    String queryNetwork(ScriptType scriptType, String scriptContent);

    Network getNetwork();

    /**
     * Get the network and add a listeners on it in order to listen changes due to virtual case script application.
     * The listeners will not be removed from the network at the end of the network loading,
     * so the user of this method must make sure to handle it on its own.
     * The contract of being notified may not be honored by all implementations (see remote service cache : listeners will not be added to the network).
     * The user must check with the cache implementation he will use.
     */
    Network getNetwork(List<NetworkListener> listeners);

    void invalidateNetworkCache();

    void addListener(ProjectCaseListener l);

    void removeListener(ProjectCaseListener l);
}
