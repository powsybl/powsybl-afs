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
import java.util.stream.Collectors;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Service
public class NodeService {

    private final NodeRepository nodeRepository;
    private final MetaDataService metaDataService;

    @Autowired
    NodeService(NodeRepository nodeRepository, MetaDataService metaDataService) {
        this.nodeRepository = Objects.requireNonNull(nodeRepository);
        this.metaDataService = Objects.requireNonNull(metaDataService);
    }

    NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        final Optional<NodeInfoEntity> byName = nodeRepository.findByName(name);
        if (byName.isPresent()) {
            return byName.get().toNodeInfo();
        }
        long creationTime = ZonedDateTime.now().toInstant().toEpochMilli();
        final NodeInfoEntity created = nodeRepository.save(new NodeInfoEntity(UUID.randomUUID().toString(), null, name, nodePseudoClass, "", creationTime, creationTime, 0));
        return created.toNodeInfo();
    }

    NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, int version, NodeGenericMetadata genericMetadata) {
        long creationTime = ZonedDateTime.now().toInstant().toEpochMilli();
        final String nodeId = UUID.randomUUID().toString();
        metaDataService.saveMetaData(nodeId, genericMetadata);
        return nodeRepository.save(new NodeInfoEntity(nodeId, parentNodeId, name, nodePseudoClass, description, creationTime, creationTime, version))
                .toNodeInfo(genericMetadata);
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

    private static PostgresAfsException createNodeNotFoundException(UUID nodeId) {
        return new PostgresAfsException("Node '" + nodeId + "' not found");
    }
}
