/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ext.base;

import com.powsybl.afs.ProjectFile;
import com.powsybl.afs.ProjectFileCreationContext;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.NetworkListener;
import com.powsybl.scripting.groovy.GroovyScriptExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
public abstract class AbstractProjectCase extends ProjectFile implements ProjectCase {

    protected AbstractProjectCase(ProjectFileCreationContext context, int version) {
        super(context, version);
    }

    @Override
    public String queryNetwork(ScriptType scriptType, String scriptContent) {
        return queryNetwork(scriptType, scriptContent, Collections.emptyList(), Collections.emptyMap());
    }

    @Override
    public String queryNetwork(ScriptType scriptType, String scriptContent, Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects) {
        Objects.requireNonNull(scriptType);
        Objects.requireNonNull(scriptContent);
        return findService(NetworkCacheService.class).queryNetwork(this, scriptType, scriptContent, extensions, contextObjects);
    }

    @Override
    public Network getNetwork() {
        return findService(NetworkCacheService.class).getNetwork(this);
    }

    @Override
    public Network getNetwork(Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects) {
        return findService(NetworkCacheService.class).getNetwork(this, extensions, contextObjects);
    }

    @Override
    public Network getNetwork(List<NetworkListener> listeners) {
        return findService(NetworkCacheService.class).getNetwork(this, listeners);
    }

    @Override
    public Network getNetwork(Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects, List<NetworkListener> listeners) {
        return findService(NetworkCacheService.class).getNetwork(this, listeners, extensions, contextObjects);
    }

    @Override
    public void invalidateNetworkCache() {
        findService(NetworkCacheService.class).invalidateCache(this);
    }
}
