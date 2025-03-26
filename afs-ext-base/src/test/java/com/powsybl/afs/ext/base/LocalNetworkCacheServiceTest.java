/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ext.base;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.ProjectFile;
import com.powsybl.afs.ProjectFileCreationContext;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.NetworkListener;
import com.powsybl.scripting.groovy.GroovyScriptExtension;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class LocalNetworkCacheServiceTest {

    @Test
    void loadNetworkFromProjectCaseExceptionTest() {
        AnonymousClass projectCase = mock(AnonymousClass.class);
        List<NetworkListener> listeners = Collections.emptyList();
        LocalNetworkCacheService localNetworkCacheService = new LocalNetworkCacheService();
        AfsException exception = assertThrows(AfsException.class, () -> localNetworkCacheService.getNetwork(projectCase, listeners));
        assertEquals("ProjectCase implementation " + AnonymousClass.class.getName() + " not supported", exception.getMessage());
    }

    private static class AnonymousClass extends ProjectFile implements ProjectCase {
        protected AnonymousClass(ProjectFileCreationContext context, int codeVersion) {
            super(context, codeVersion);
        }

        @Override
        public String queryNetwork(ScriptType scriptType, String scriptContent) {
            return "";
        }

        @Override
        public String queryNetwork(ScriptType scriptType, String scriptContent, Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects) {
            return "";
        }

        @Override
        public Network getNetwork() {
            return null;
        }

        @Override
        public Network getNetwork(Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects) {
            return null;
        }

        @Override
        public Network getNetwork(List<NetworkListener> listeners) {
            return null;
        }

        @Override
        public Network getNetwork(Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects, List<NetworkListener> listeners) {
            return null;
        }

        @Override
        public void invalidateNetworkCache() {
            // Nothing here
        }

        @Override
        public void addListener(ProjectCaseListener l) {
            // Nothing here
        }

        @Override
        public void removeListener(ProjectCaseListener l) {
            // Nothing here
        }
    }
}
