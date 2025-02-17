/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.powsybl.iidm.network.Network;
import com.powsybl.scripting.groovy.GroovyScriptExtension;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.MissingMethodException;
import groovy.lang.MissingPropertyException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 *
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public final class ScriptUtils {

    private static final String SCRIPT_FILE_NAME = "test";

    private ScriptUtils() {
    }

    private static ScriptResult<Object> runGroovyScript(Network network, Reader reader, Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects) {
        String output = "";
        ScriptError error = null;
        Object value = null;
        try (StringWriter outputWriter = new StringWriter()) {
            // put network in the binding so that it is accessible from the script
            Binding binding = new Binding();
            binding.setProperty("network", network);
            binding.setProperty("out", outputWriter);

            // Bindings through extensions
            extensions.forEach(extension -> extension.load(binding, contextObjects));

            CompilerConfiguration config = new CompilerConfiguration();
            GroovyShell shell = new GroovyShell(binding, config);
            value = shell.evaluate(reader, SCRIPT_FILE_NAME);
            outputWriter.flush();
            output = outputWriter.toString();
        } catch (MultipleCompilationErrorsException e) {
            error = ScriptError.fromGroovyException(e);
        } catch (MissingPropertyException | MissingMethodException e) {
            error = ScriptError.fromGroovyException(e, SCRIPT_FILE_NAME);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ScriptResult<>(value, output, error);
    }

    public static ScriptResult<Object> runScript(Network network, ScriptType scriptType, String scriptContent, Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects) {
        try (Reader reader = new StringReader(scriptContent)) {
            if (scriptType == ScriptType.GROOVY) {
                return runGroovyScript(network, reader, extensions, contextObjects);
            } else {
                throw new AssertionError("Script type " + scriptType + " not supported");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
