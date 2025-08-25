/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mixed.storage;

import com.powsybl.afs.mixed.LocalDataAppFileSystemConfig;
import com.powsybl.afs.storage.AbstractAppStorage;
import com.powsybl.afs.storage.NodeDependency;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class LocalDataAppStorage extends AbstractAppStorage {

    private static final String UNSUPPORTED_MSG = "LocalDataAppStorage not support node/time-series operation.";
    private static final UnsupportedOperationException UNSUPPORTED_OPERATION_EXCEPTION = new UnsupportedOperationException(UNSUPPORTED_MSG);

    private final LocalDataAppFileSystemConfig config;
    private final Path rootDir;

    public LocalDataAppStorage(LocalDataAppFileSystemConfig config) {
        this.config = Objects.requireNonNull(config);
        rootDir = config.getRootDir();
    }

    @Override
    public String getFileSystemName() {
        return config.getDriveName();
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    public NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        return null;
    }

    @Override
    public NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, int version, NodeGenericMetadata genericMetadata) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public boolean isWritable(String nodeId) {
        return false;
    }

    @Override
    public NodeInfo getNodeInfo(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public void setDescription(String nodeId, String description) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public void updateModificationTime(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public List<NodeInfo> getChildNodes(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Optional<NodeInfo> getChildNode(String nodeId, String name) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Optional<NodeInfo> getParentNode(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public void setParentNode(String nodeId, String newParentNodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public String deleteNode(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Optional<InputStream> readBinaryData(String nodeId, String name) {
        final Path resolve = rootDir.resolve(config.getDriveName()).resolve(nodeId).resolve(name);
        if (Files.exists(resolve)) {
            try {
                return Optional.of(new FileInputStream(resolve.toFile()));
            } catch (FileNotFoundException e) {
                throw new UncheckedIOException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public OutputStream writeBinaryData(String nodeId, String name) {
        final Path resolve = rootDir.resolve(config.getDriveName()).resolve(nodeId);
        try {
            Files.createDirectories(resolve);
            return new FileOutputStream(resolve.resolve(name).toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean dataExists(String nodeId, String name) {
        return false;
    }

    @Override
    public Set<String> getDataNames(String nodeId) {
        return null;
    }

    @Override
    public boolean removeData(String nodeId, String name) {
        return false;
    }

    @Override
    public void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Set<String> getTimeSeriesNames(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public void clearTimeSeries(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public void addDependency(String nodeId, String name, String toNodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Set<NodeInfo> getDependencies(String nodeId, String name) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Set<NodeDependency> getDependencies(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public Set<NodeInfo> getBackwardDependencies(String nodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public void removeDependency(String nodeId, String name, String toNodeId) {
        throw UNSUPPORTED_OPERATION_EXCEPTION;
    }

    @Override
    public void flush() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
        flush();
    }

    private Void throwUnsupportedOperation() {
        throw new UnsupportedOperationException("LocalDataAppStorage not support node/time-series operation.");
    }
}
