/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.powsybl.afs.postgres.jpa.NodeDataEntity;
import com.powsybl.afs.postgres.jpa.NodeDataRepository;
import com.powsybl.afs.postgres.jpa.TimeSeriesMetadataEntity;
import com.powsybl.afs.postgres.jpa.TimeSeriesMetadataRepository;
import com.powsybl.afs.storage.AbstractAppStorage;
import com.powsybl.afs.storage.NodeDependency;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
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
    private final TimeSeriesMetadataRepository timeSeriesMetadataRepository;

    private static final int ROOT_NODE_VERSION = 0;

    @Autowired
    public PostgresAppStorage(NodeService nodeService,
                              NodeDataRepository nodeDataRepository,
                              TimeSeriesMetadataRepository timeSeriesMetadataRepository) {
        this.nodeService = Objects.requireNonNull(nodeService);
        this.nodeDataRepository = Objects.requireNonNull(nodeDataRepository);
        this.timeSeriesMetadataRepository = Objects.requireNonNull(timeSeriesMetadataRepository);
    }

    @Override
    public String getFileSystemName() {
        return null;
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    public NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        return nodeService.createRootNodeIfNotExists(name, nodePseudoClass);
    }

    @Override
    public NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, int version, NodeGenericMetadata genericMetadata) {
        return nodeService.createNode(parentNodeId, name, nodePseudoClass, description, version, genericMetadata);
    }

    @Override
    public boolean isWritable(String nodeId) {
        return false;
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
        nodeDataRepository.deleteByNodeId(nodeId);
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
        nodeDataRepository.deleteByNodeIdAndKey(nodeId, name);
        return true;
    }

    @Override
    public void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        TimeSeriesMetadataEntity entity = new TimeSeriesMetadataEntity();
        entity.setType(metadata.getDataType().name());
        entity.setName(metadata.getName());
        entity.setNodeId(nodeId);
        timeSeriesMetadataRepository.save(entity);
    }

    @Override
    public Set<String> getTimeSeriesNames(String nodeId) {
        final Iterable<TimeSeriesMetadataEntity> allByNodeId = timeSeriesMetadataRepository.findAllByNodeId(nodeId);
        Set<String> set = new HashSet<>();
        allByNodeId.forEach(i -> set.add(i.getName()));
        return set;
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
    public void flush() {

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
