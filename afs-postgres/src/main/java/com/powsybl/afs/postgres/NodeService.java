/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.powsybl.afs.postgres.jpa.MetaDataService;
import com.powsybl.afs.postgres.jpa.NodeInfoEntity;
import com.powsybl.afs.postgres.jpa.NodeRepository;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Service
class NodeService {

    private final NodeRepository nodeRepository;
    private final MetaDataService metaDataService;

    private BiConsumer<String, String> nodeCreated;
    private Consumer<String> nodeConsistentTrue;
    private BiConsumer<String, String> descriptionUpdated;

    @Autowired
    NodeService(NodeRepository nodeRepository, MetaDataService metaDataService) {
        this.nodeRepository = Objects.requireNonNull(nodeRepository);
        this.metaDataService = Objects.requireNonNull(metaDataService);
    }

    void setNodeCreated(BiConsumer<String, String> nodeCreated) {
        this.nodeCreated = nodeCreated;
    }

    void setNodeConsistence(Consumer<String> nodeConsistent) {
        this.nodeConsistentTrue = nodeConsistent;
    }

    public void setDescriptionUpdated(BiConsumer<String, String> descriptionUpdated) {
        this.descriptionUpdated = descriptionUpdated;
    }

    NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        final Optional<NodeInfoEntity> byName = nodeRepository.findByName(name);
        if (byName.isPresent()) {
            return byName.get().toNodeInfo();
        }
        final NodeInfo node = createNode(null, name, nodePseudoClass, "", true, 0, new NodeGenericMetadata());
        return node;
    }

    NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, boolean consistence, int version, NodeGenericMetadata genericMetadata) {
        long creationTime = ZonedDateTime.now().toInstant().toEpochMilli();
        final String nodeId = UUID.randomUUID().toString();
        metaDataService.saveMetaData(nodeId, genericMetadata);
        final NodeInfo nodeInfo = nodeRepository.save(new NodeInfoEntity(nodeId, parentNodeId, name, nodePseudoClass, description, creationTime, creationTime, version).setConsistence(consistence))
                .toNodeInfo(genericMetadata);
        nodeCreated.accept(nodeId, parentNodeId);
        if (consistence) {
            nodeConsistentTrue.accept(nodeId);
        }
        return nodeInfo;
    }

    NodeInfo getNodeInfo(String nodeId) {
        final Optional<NodeInfoEntity> byId = nodeRepository.findById(nodeId);
        if (byId.isPresent()) {
            final NodeInfoEntity nodeInfoEntity = byId.get();
            final NodeGenericMetadata metadataByNodeId = metaDataService.getMetaDataByNodeId(nodeInfoEntity.getId());
            return nodeInfoEntity.toNodeInfo(metadataByNodeId);
        }
        throw createNodeNotFoundException(UUID.fromString(nodeId));
    }

    void setDescription(String nodeId, String description) {
        nodeRepository.updateDescriptionAndModificationTimeById(nodeId, description, ZonedDateTime.now().toInstant().toEpochMilli());
        descriptionUpdated.accept(nodeId, description);
    }

    void updateModificationTime(String nodeId) {
        nodeRepository.updateModificationTimeById(nodeId, ZonedDateTime.now().toInstant().toEpochMilli());
    }

    List<NodeInfo> getChildNodes(String nodeId) {
        return nodeRepository.findAllByParentId(nodeId).stream()
                .map(NodeInfoEntity::getId)
                .map(this::getNodeInfo)
                .collect(Collectors.toList());
    }

    Optional<NodeInfo> getChildNode(String nodeId, String name) {
        return nodeRepository.findByParentIdAndName(nodeId, name)
                .map(nodeInfoEntity -> getNodeInfo(nodeInfoEntity.getId()));
    }

    Optional<NodeInfo> getParentNode(String nodeId) {
        return nodeRepository.findById(nodeId)
                .map(NodeInfoEntity::getParentId).map(this::getNodeInfo);
    }

    void setParentNode(String nodeId, String newParentNodeId) {
        nodeRepository.updateParentById(nodeId, ZonedDateTime.now().toInstant().toEpochMilli(), newParentNodeId);
    }

    String deleteNode(String nodeId) {
        final Optional<NodeInfo> parentNode = getParentNode(nodeId);
        final List<NodeInfo> childNodes = getChildNodes(nodeId);
        childNodes.forEach(id -> deleteNode(id.getId()));
        nodeRepository.deleteById(nodeId);
        metaDataService.deleteMetaDate(nodeId);
        return parentNode.map(NodeInfo::getId).orElse(null);
    }

    boolean isConsistent(String nodeId) {
        return nodeRepository.findById(nodeId).map(NodeInfoEntity::isConsistence).orElseThrow(() -> createNodeNotFoundException(nodeId));
    }

    private static PostgresAfsException createNodeNotFoundException(UUID nodeId) {
        return createNodeNotFoundException(nodeId.toString());
    }

    private static PostgresAfsException createNodeNotFoundException(String nodeId) {
        return new PostgresAfsException("Node '" + nodeId + "' not found");
    }

    List<NodeInfo> getInconsistentNodes() {
        return nodeRepository.findAllByConsistenceFalse().
                stream().map(e -> getNodeInfo(e.getId()))
                .collect(Collectors.toList());
    }

    void setConsistent(String nodeId) {
        nodeRepository.updateConsistenceById(nodeId, true);
        nodeConsistentTrue.accept(nodeId);
    }
}
