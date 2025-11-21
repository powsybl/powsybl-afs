/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.powsybl.afs.ProjectFile;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.NetworkListener;
import com.powsybl.scripting.groovy.GroovyScriptExtension;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Provides caching capabilities for loaded {@code Network} objects.
 *
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public interface NetworkCacheService {

    <T extends ProjectFile & ProjectCase> Network getNetwork(T projectCase);

    <T extends ProjectFile & ProjectCase> Network getNetwork(T projectCase, Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects);

    <T extends ProjectFile & ProjectCase> Network getNetwork(T projectCase, List<NetworkListener> listeners);

    <T extends ProjectFile & ProjectCase> Network getNetwork(T projectCase, List<NetworkListener> listeners, Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects);

    <T extends ProjectFile & ProjectCase> String queryNetwork(T projectCase, ScriptType scriptType, String scriptContent);

    <T extends ProjectFile & ProjectCase> String queryNetwork(T projectCase, ScriptType scriptType, String scriptContent, Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects);

    <T extends ProjectFile & ProjectCase> void invalidateCache(T projectCase);

    <T extends ProjectFile & ProjectCase> void addListener(T projectCase, ProjectCaseListener listener);

    <T extends ProjectFile & ProjectCase> void removeListener(T projectCase, ProjectCaseListener listener);

    default <T extends ProjectFile & ProjectCase> String getOutput(T projectCase) {
        return StringUtils.EMPTY;
    }

    default <T extends ProjectFile & ProjectCase> String getOutput(T projectCase, Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects) {
        return StringUtils.EMPTY;
    }
}
