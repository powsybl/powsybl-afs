/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.powsybl.afs.postgres.jpa.NodeDataEntity;
import com.powsybl.afs.postgres.jpa.NodeDataRepository;
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.events.*;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Service
public class PostgresAppStorage extends AbstractAppStorage {

    private final NodeService nodeService;
    private final NodeDataRepository nodeDataRepository;
    private final TimeSeriesService tsService;

    private final String fileSystemName;

    private static final int ROOT_NODE_VERSION = 0;

    @Autowired
    public PostgresAppStorage(String fileSystemName,
                              NodeService nodeService,
                              NodeDataRepository nodeDataRepository,
                              TimeSeriesService tsService,
                              EventsBus eventsBus) {
        this.fileSystemName = Objects.requireNonNull(fileSystemName);
        this.nodeService = Objects.requireNonNull(nodeService);
        this.nodeDataRepository = Objects.requireNonNull(nodeDataRepository);
        this.tsService = Objects.requireNonNull(tsService);
        this.eventsBus = Objects.requireNonNull(eventsBus);
        bindListener();
    }

    private void bindListener() {
        nodeService.setNodeCreated((nodeId, parentNodeId) -> pushEvent(new NodeCreated(nodeId, parentNodeId), APPSTORAGE_NODE_TOPIC));
        nodeService.setNodeConsistentTrue(id -> pushEvent(new NodeConsistent(id), APPSTORAGE_NODE_TOPIC));
        nodeService.setDescriptionUpdated((id, desc) -> pushEvent(new NodeDescriptionUpdated(id, desc), APPSTORAGE_NODE_TOPIC));
        nodeService.setDepAdded((id, depName) -> pushEvent(new DependencyAdded(id, depName), APPSTORAGE_DEPENDENCY_TOPIC));
        nodeService.setBDepAdded((id, depName) -> pushEvent(new BackwardDependencyAdded(id, depName), APPSTORAGE_DEPENDENCY_TOPIC));
        nodeService.setDepRemoved((id, depName) -> pushEvent(new DependencyRemoved(id, depName), APPSTORAGE_DEPENDENCY_TOPIC));
        nodeService.setBDepRemoved((id, depName) -> pushEvent(new BackwardDependencyRemoved(id, depName), APPSTORAGE_DEPENDENCY_TOPIC));
        nodeService.setNodeRemoved((id, pid) -> pushEvent(new NodeRemoved(id, pid), APPSTORAGE_NODE_TOPIC));
        nodeService.setParentChanged((id, opid, npid) -> pushEvent(new ParentChanged(id, opid, npid), APPSTORAGE_NODE_TOPIC));
        nodeService.setNodeNameUpdated((id, name) -> pushEvent(new NodeNameUpdated(id, name), APPSTORAGE_NODE_TOPIC));
        nodeService.setNodeMetadataUpdated((id, data) -> pushEvent(new NodeMetadataUpdated(id, data), APPSTORAGE_NODE_TOPIC));
        tsService.setTimeSeriesCreated((id, name) -> pushEvent(new TimeSeriesCreated(id, name), APPSTORAGE_TIMESERIES_TOPIC));
        tsService.setTimeSeriesDataUpdated((id, name) -> pushEvent(new TimeSeriesDataUpdated(id, name), APPSTORAGE_NODE_TOPIC));
        tsService.setTimeSeriesCleared(id -> pushEvent(new TimeSeriesCleared(id), APPSTORAGE_TIMESERIES_TOPIC));
    }

    @Override
    public String getFileSystemName() {
        return fileSystemName;
    }

    @Override
    public boolean isConsistent(String nodeId) {
        return nodeService.isConsistent(nodeId);
    }

    @Override
    public List<NodeInfo> getInconsistentNodes() {
        return nodeService.getInconsistentNodes();
    }

    @Override
    public void setMetadata(String nodeId, NodeGenericMetadata genericMetadata) {
        nodeService.setMetadata(nodeId, genericMetadata);
    }

    @Override
    public void setConsistent(String nodeId) {
        nodeService.setConsistent(nodeId);
    }

    @Override
    public void renameNode(String nodeId, String name) {
        nodeService.renameNode(nodeId, name);
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    public NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        final NodeInfo info = nodeService.createRootNodeIfNotExists(name, nodePseudoClass);
        return info;
    }

    @Override
    public NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, int version, NodeGenericMetadata genericMetadata) {
        return nodeService.createNode(parentNodeId, name, nodePseudoClass, description, false, version, genericMetadata);
    }

    @Override
    public boolean isWritable(String nodeId) {
        // TODO
        return true;
    }

    @Override
    public NodeInfo getNodeInfo(String nodeId) {
        return nodeService.getNodeInfo(nodeId);
    }

    @Override
    public void setDescription(String nodeId, String description) {
        nodeService.setDescription(nodeId, description);
    }

    @Override
    public void updateModificationTime(String nodeId) {
        nodeService.updateModificationTime(nodeId);
    }

    @Override
    public List<NodeInfo> getChildNodes(String nodeId) {
        return nodeService.getChildNodes(nodeId);
    }

    @Override
    public Optional<NodeInfo> getChildNode(String nodeId, String name) {
        return nodeService.getChildNode(nodeId, name);
    }

    @Override
    public Optional<NodeInfo> getParentNode(String nodeId) {
        return nodeService.getParentNode(nodeId);
    }

    @Override
    public void setParentNode(String nodeId, String newParentNodeId) {
        nodeService.setParentNode(nodeId, newParentNodeId);
    }

    @Override
    public String deleteNode(String nodeId) {
        final String s = nodeService.deleteNode(nodeId);
        // TODO node id foreign key to auto delete
//        nodeDataRepository.deleteByNodeId(nodeId);
        return s;
    }

    @Override
    public Optional<InputStream> readBinaryData(String nodeId, String name) {
        return nodeDataRepository.findByNodeIdAndKey(nodeId, name)
                .map(e -> new ByteArrayInputStream(e.getBinaryData()));
    }

    @Override
    public OutputStream writeBinaryData(String nodeId, String name) {
        final BinaryDataOutputStream os = new BinaryDataOutputStream(nodeId, name);
        pushEvent(new NodeDataUpdated(nodeId, name), APPSTORAGE_NODE_TOPIC);
        return os;
    }

    @Override
    public boolean dataExists(String nodeId, String name) {
        return nodeDataRepository.findByNodeIdAndKey(nodeId, name).isPresent();
    }

    @Override
    public Set<String> getDataNames(String nodeId) {
        return nodeDataRepository.findAllByNodeId(nodeId).stream()
                .map(NodeDataEntity::getKey).collect(Collectors.toSet());
    }

    @Override
    public boolean removeData(String nodeId, String name) {
        if (!dataExists(nodeId, name)) {
            return false;
        }
        nodeDataRepository.deleteByNodeIdAndKey(nodeId, name);
        pushEvent(new NodeDataRemoved(nodeId, name), APPSTORAGE_NODE_TOPIC);
        return true;
    }

    @Override
    public void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        tsService.createTimeSeries(nodeId, metadata);
    }

    @Override
    public Set<String> getTimeSeriesNames(String nodeId) {
        return tsService.getTimeSeriesNames(nodeId);
    }

    @Override
    public boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        return tsService.timeSeriesExists(nodeId, timeSeriesName);
    }

    @Override
    public List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {
        return tsService.getTimeSeriesMetadata(nodeId, timeSeriesNames);
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        return tsService.getTimeSeriesDataVersions(nodeId);
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        return tsService.getTimeSeriesDataVersions(nodeId, timeSeriesName);
    }

    @Override
    public Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        return tsService.getDoubleTimeSeriesData(nodeId, timeSeriesNames, version);
    }

    @Override
    public void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {
        tsService.addDoubleTimeSeriesData(nodeId, version, timeSeriesName, chunks);
    }

    @Override
    public Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        return tsService.getStringTimeSeriesData(nodeId, timeSeriesNames, version);
    }

    @Override
    public void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {
        tsService.addStringTimeSeriesData(nodeId, version, timeSeriesName, chunks);
    }

    @Override
    public void clearTimeSeries(String nodeId) {
        tsService.clearTimeSeries(nodeId);
    }

    @Override
    public void addDependency(String nodeId, String name, String toNodeId) {
        nodeService.addDependency(nodeId, name, toNodeId);
    }

    @Override
    public Set<NodeInfo> getDependencies(String nodeId, String name) {
        return nodeService.getDependencies(nodeId, name);
    }

    @Override
    public Set<NodeDependency> getDependencies(String nodeId) {
        return nodeService.getDependencies(nodeId);
    }

    @Override
    public Set<NodeInfo> getBackwardDependencies(String nodeId) {
        return nodeService.getBackwardDependencies(nodeId);
    }

    @Override
    public void removeDependency(String nodeId, String name, String toNodeId) {
        nodeService.removeDependency(nodeId, name, toNodeId);
    }

    @Override
    public void flush() {
        eventsBus.flush();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }

    class BinaryDataOutputStream extends OutputStream {

        private final String nodeId;
        private final String name;
        private final ByteArrayOutputStream baos;

        BinaryDataOutputStream(String nodeId, String name) {
            this.nodeId = nodeId;
            this.name = name;
            baos = new ByteArrayOutputStream();
        }

        @Override
        public void write(int b) throws IOException {
            baos.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            baos.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            System.out.println("in flush");

            super.flush();
        }

        @Override
        public void close() throws IOException {
            baos.close();
            final NodeDataEntity nodeDataEntity = new NodeDataEntity();
            nodeDataEntity.setNodeId(nodeId);
            nodeDataEntity.setKey(name);
            nodeDataEntity.setBinaryData(baos.toByteArray());
            nodeDataRepository.save(nodeDataEntity);
            super.close();
        }
    }

}
