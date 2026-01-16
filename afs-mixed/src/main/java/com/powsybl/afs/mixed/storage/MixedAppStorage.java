/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.mixed.storage;

import com.powsybl.afs.AppData;
import com.powsybl.afs.mixed.MixedAppFileSystemConfig;
import com.powsybl.afs.storage.*;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class MixedAppStorage extends AbstractAppStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(MixedAppStorage.class);

    private final String nodeStorageName;
    private final String dataStorageName;

    private AppStorage nodeStorage;
    private AppStorage dataStorage;

    private AppData appData;

    public MixedAppStorage(MixedAppFileSystemConfig config) {
        Objects.requireNonNull(config);
        this.nodeStorageName = config.getNodeStorageDriveName();
        this.dataStorageName = config.getDataStorageDriveName();
    }

    public void setAppData(AppData appData) {
        this.appData = appData;
    }

    private AppStorage getNodeStorage() {
        if (nodeStorage == null) {
            nodeStorage = appData.getRemotelyAccessibleStorage(nodeStorageName);
        }
        return nodeStorage;
    }

    private AppStorage getDataStorage() {
        if (dataStorage == null) {
            dataStorage = appData.getRemotelyAccessibleStorage(dataStorageName);
        }
        return dataStorage;
    }

    @Override
    public String getFileSystemName() {
        return getNodeStorage().getFileSystemName();
    }

    @Override
    public boolean isRemote() {
        return getNodeStorage().isRemote();
    }

    @Override
    public NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        return getNodeStorage().createRootNodeIfNotExists(name, nodePseudoClass);
    }

    @Override
    public NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, int version, NodeGenericMetadata genericMetadata) {
        return getNodeStorage().createNode(parentNodeId, name, nodePseudoClass, description, version, genericMetadata);
    }

    @Override
    public boolean isWritable(String nodeId) {
        return getNodeStorage().isWritable(nodeId) && getDataStorage().isWritable(nodeId);
    }

    @Override
    public NodeInfo getNodeInfo(String nodeId) {
        return getNodeStorage().getNodeInfo(nodeId);
    }

    @Override
    public void setDescription(String nodeId, String description) {
        getNodeStorage().setDescription(nodeId, description);
    }

    @Override
    public void updateModificationTime(String nodeId) {
        getNodeStorage().updateModificationTime(nodeId);
    }

    @Override
    public List<NodeInfo> getChildNodes(String nodeId) {
        return getNodeStorage().getChildNodes(nodeId);
    }

    @Override
    public Optional<NodeInfo> getChildNode(String nodeId, String name) {
        return getNodeStorage().getChildNode(nodeId, name);
    }

    @Override
    public Optional<NodeInfo> getParentNode(String nodeId) {
        return getNodeStorage().getParentNode(nodeId);
    }

    @Override
    public void setParentNode(String nodeId, String newParentNodeId) {
        getNodeStorage().setParentNode(nodeId, newParentNodeId);
    }

    @Override
    public void setConsistent(String nodeId) {
        getNodeStorage().setConsistent(nodeId);
    }

    @Override
    public String deleteNode(String nodeId) {
        return getNodeStorage().deleteNode(nodeId);
    }

    @Override
    public Optional<InputStream> readBinaryData(String nodeId, String name) {
        return getDataStorage().readBinaryData(nodeId, name);
    }

    @Override
    public OutputStream writeBinaryData(String nodeId, String name) {
        return getDataStorage().writeBinaryData(nodeId, name);
    }

    @Override
    public boolean dataExists(String nodeId, String name) {
        return getDataStorage().dataExists(nodeId, name);
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

    }

    @Override
    public Set<String> getTimeSeriesNames(String nodeId) {
        return null;
    }

    @Override
    public boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        return false;
    }

    @Override
    public List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {
        return null;
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        return null;
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        return null;
    }

    @Override
    public Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        return null;
    }

    @Override
    public void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {

    }

    @Override
    public Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        return null;
    }

    @Override
    public void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {

    }

    @Override
    public void clearTimeSeries(String nodeId) {

    }

    @Override
    public void addDependency(String nodeId, String name, String toNodeId) {

    }

    @Override
    public Set<NodeInfo> getDependencies(String nodeId, String name) {
        return null;
    }

    @Override
    public Set<NodeDependency> getDependencies(String nodeId) {
        return null;
    }

    @Override
    public Set<NodeInfo> getBackwardDependencies(String nodeId) {
        return null;
    }

    @Override
    public void removeDependency(String nodeId, String name, String toNodeId) {

    }

    @Override
    public EventsBus getEventsBus() {
        return null;
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

    }
}
