/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.ProjectFile;
import com.powsybl.commons.datasource.ReadOnlyDataSource;
import com.powsybl.iidm.network.Importer;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.NetworkListener;
import groovy.json.JsonOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class LocalNetworkCacheService implements NetworkCacheService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalNetworkCacheService.class);

    private final ScriptCache<ProjectFile, Network, ProjectCaseListener> cache;

    public LocalNetworkCacheService() {
        cache = new ScriptCache<>(50, 1, projectFile -> {
            UUID taskId = projectFile.startTask();
            try {
                projectFile.createLogger(taskId).log("Loading network...");
                return loadNetworkFromProjectCase((ProjectCase) projectFile);
            } finally {
                projectFile.stopTask(taskId);
            }
        }, (result, listeners) -> {
            for (ProjectCaseListener listener : listeners) {
                listener.networkUpdated();
            }
        });
    }

    private static ScriptResult<Network> loadNetworkFromImportedCase(ImportedCase importedCase, List<NetworkListener> listeners) {
        LOGGER.info("Loading network of project case {}", importedCase.getId());

        Importer importer = importedCase.getImporter();
        ReadOnlyDataSource dataSource = importedCase.getDataSource();
        Properties parameters = importedCase.getParameters();
        Network network = importer.importData(dataSource, parameters);
        listeners.forEach(network::addListener);
        return ScriptResult.of(network);
    }

    private static ScriptResult<Network> applyScript(Network network, String previousScriptOutput, ModificationScript script) {
        ScriptResult<Object> result = ScriptUtils.runScript(network, script.getScriptType(), script.readScript(true));
        if (result.getError() == null) {
            return new ScriptResult<>(network, previousScriptOutput + result.getOutput(), null);
        } else {
            // return an empty network
            return new ScriptResult<>(null, result.getOutput(), result.getError());
        }
    }

    private static ScriptResult<Network> loadNetworkFromVirtualCase(VirtualCase virtualCase, List<NetworkListener> listeners) {
        ProjectCase baseCase = (ProjectCase) virtualCase.getCase()
                                                        .orElseThrow(() -> new AfsException("Case link is dead"));

        ScriptResult<Network> network = loadNetworkFromProjectCase(baseCase, listeners);

        if (network.getError() != null) {
            return network;
        }

        ModificationScript script = virtualCase.getScript()
                                               .orElseThrow(VirtualCase::createScriptLinkIsDeadException);

        LOGGER.info("Applying script to network of project case {}", virtualCase.getId());

        return applyScript(network.getValue(), network.getOutput(), script);
    }

    private static ScriptResult<Network> loadNetworkFromProjectCase(ProjectCase projectCase) {
        return loadNetworkFromProjectCase(projectCase, Collections.emptyList());
    }

    private static ScriptResult<Network> loadNetworkFromProjectCase(ProjectCase projectCase, List<NetworkListener> listeners) {
        if (projectCase instanceof ImportedCase) {
            return loadNetworkFromImportedCase((ImportedCase) projectCase, listeners);
        } else if (projectCase instanceof VirtualCase) {
            return loadNetworkFromVirtualCase((VirtualCase) projectCase, listeners);
        } else {
            throw new AssertionError("ProjectCase implementation " + projectCase.getClass().getName() + " not supported");
        }
    }

    @Override
    public <T extends ProjectFile & ProjectCase> String queryNetwork(T projectCase, ScriptType scriptType, String scriptContent) {
        Objects.requireNonNull(projectCase);
        Objects.requireNonNull(scriptType);
        Objects.requireNonNull(scriptContent);

        Network network = getNetwork(projectCase);

        ScriptResult<Object> result = ScriptUtils.runScript(network, ScriptType.GROOVY, scriptContent);
        if (result.getError() != null) {
            throw new ScriptException(projectCase, result.getError());
        }
        return JsonOutput.toJson(result.getValue());
    }

    @Override
    public <T extends ProjectFile & ProjectCase> Network getNetwork(T projectCase) {
        return cache.get(projectCase).getValueOrThrowIfError(projectCase);
    }

    @Override
    public <T extends ProjectFile & ProjectCase> Network getNetwork(T projectCase, List<NetworkListener> listeners) {
        ScriptResult<Network> network = loadNetworkFromProjectCase(projectCase, listeners);
        return network.getValueOrThrowIfError(projectCase);
    }

    @Override
    public <T extends ProjectFile & ProjectCase> String getOutput(T projectCase) {
        return cache.get(projectCase).getOutput();
    }

    @Override
    public <T extends ProjectFile & ProjectCase> void invalidateCache(T projectCase) {
        cache.invalidate(projectCase);
    }

    @Override
    public <T extends ProjectFile & ProjectCase> void addListener(T projectCase, ProjectCaseListener listener) {
        cache.addListener(projectCase, listener);
    }

    @Override
    public <T extends ProjectFile & ProjectCase> void removeListener(T projectCase, ProjectCaseListener listener) {
        cache.removeListener(projectCase, listener);
    }
}
