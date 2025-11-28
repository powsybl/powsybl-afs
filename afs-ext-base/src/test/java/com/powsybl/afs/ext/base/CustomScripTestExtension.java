/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.ext.base;

import com.powsybl.scripting.groovy.GroovyScriptExtension;
import groovy.lang.Binding;

import java.util.Map;

/**
 * @author Matthieu SAUR {@literal <matthieu.saur at rte-france.com>}
 */
public class CustomScripTestExtension implements GroovyScriptExtension {
    public CustomScripTestExtension() { /* */
    }

    @Override
    public void load(Binding binding, Map<Class<?>, Object> map) {
        binding.setVariable("customOut", binding.getProperty("out"));
    }

    @Override
    public void unload() { /* */ }
}
