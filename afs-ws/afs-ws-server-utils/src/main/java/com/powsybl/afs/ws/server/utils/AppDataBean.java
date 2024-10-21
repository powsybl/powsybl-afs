/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server.utils;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.AppData;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.ProjectFile;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.commons.PowsyblException;
import com.powsybl.computation.DefaultComputationManagerConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@Named
@Singleton
public class AppDataBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppDataBean.class);

    AppData appData;

    protected DefaultComputationManagerConfig config;

    public AppData getAppData() {
        return appData;
    }

    public void setAppData(AppData appData) {
        this.appData = appData;
    }

    public AppStorage getStorage(String fileSystemName) {
        AppStorage storage = appData.getRemotelyAccessibleStorage(fileSystemName);
        if (storage == null) {
            throw new WebApplicationException("App file system '" + fileSystemName + "' not found",
                    Response.Status.NOT_FOUND);
        }
        return storage;
    }

    public AppFileSystem getFileSystem(String name) {
        Objects.requireNonNull(appData);
        Objects.requireNonNull(name);
        AppFileSystem fileSystem = appData.getFileSystem(name);
        if (fileSystem == null) {
            throw new WebApplicationException("App file system '" + name + "' not found", Response.Status.NOT_FOUND);
        }
        return fileSystem;
    }

    public <T extends ProjectFile> T getProjectFile(String fileSystemName, String nodeId, Class<T> clazz) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(clazz);
        AppFileSystem fileSystem = getFileSystem(fileSystemName);
        return fileSystem.findProjectFile(nodeId, clazz);
    }

    public <T extends ProjectFile, U> U getProjectFile(String fileSystemName, String nodeId, Class<T> clazz, Class<U> clazz2) {
        T projectFile = getProjectFile(fileSystemName, nodeId, clazz);
        if (!(clazz2.isAssignableFrom(projectFile.getClass()))) {
            throw new AfsException("Project file '" + nodeId + "' is not a " + clazz2.getName());
        }
        return (U) projectFile;
    }

    @PostConstruct
    public void init() {
        config = DefaultComputationManagerConfig.load();
        appData = new AppData(config.createShortTimeExecutionComputationManager(), config.createLongTimeExecutionComputationManager());
    }

    @PreDestroy
    public void clean() {
        if (appData != null) {
            appData.close();
            appData.getShortTimeExecutionComputationManager().close();
            if (appData.getLongTimeExecutionComputationManager() != null) {
                appData.getLongTimeExecutionComputationManager().close();
            }
        }
    }

    /**
     * This method reinit only the computation manager (no effect on other connexions with other backends)
     * @param throwException if {@code true}, throw the exception caught, else only log it
     */
    public void reinitComputationManager(boolean throwException) {
        // If possible, close the existing connections
        try {
            if (appData.getShortTimeExecutionComputationManager() != null) {
                appData.getShortTimeExecutionComputationManager().close();
            }
        } catch (Exception e) {
            if (throwException) {
                throw new PowsyblException("Error while closing existing connection to the short-time execution computation manager", e);
            } else {
                LOGGER.warn("shortTimeExecutionComputationManager is not in a closable state. Had exception '{}' while trying to close it. It will be reinitialized anyway.", e.getMessage());
            }
        }
        try {
            if (appData.getLongTimeExecutionComputationManager() != null) {
                appData.getLongTimeExecutionComputationManager().close();
            }
        } catch (Exception e) {
            if (throwException) {
                throw new PowsyblException("Error while closing existing connection to the long-time execution computation manager", e);
            } else {
                LOGGER.warn("longTimeExecutionComputationManager is not in a closable state. Had exception '{}' while trying to close it. It will be reinitialized anyway.", e.getMessage());
            }
        }

        // Should never be null !
        if (config == null) {
            config = DefaultComputationManagerConfig.load();
        }

        // Open new connections
        appData.setShortTimeExecutionComputationManager(config.createShortTimeExecutionComputationManager());
        appData.setLongTimeExecutionComputationManager(config.createLongTimeExecutionComputationManager());
    }
}
