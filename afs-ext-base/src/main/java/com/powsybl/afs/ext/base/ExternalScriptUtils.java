package com.powsybl.afs.ext.base;

import com.powsybl.iidm.network.Network;
import com.powsybl.scripting.groovy.GroovyScriptExtension;

public final class ExternalScriptUtils {

    private ExternalScriptUtils() {
    }

    public static ScriptResult<Object> runExternalScript(Network network, ScriptType scriptType, String scriptContent) {
        return ScriptUtils.runScript(network, scriptType, scriptContent);
    }

    public static ScriptResult<Object> runExternalScript(Network network, ScriptType scriptType, String scriptContent, Iterable<GroovyScriptExtension> extensions) {
        return ScriptUtils.runScript(network, scriptType, scriptContent, extensions);
    }

}
