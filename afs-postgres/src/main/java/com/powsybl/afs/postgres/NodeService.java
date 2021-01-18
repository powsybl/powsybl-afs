/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.google.common.base.Strings;
import com.powsybl.afs.postgres.jpa.*;
import com.powsybl.afs.storage.NodeDependency;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import lombok.AccessLevel;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.*;
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
    private final DependencyRepository depRepository;

    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> nodeCreated;
    @Setter(AccessLevel.PACKAGE)
    private Consumer<String> nodeConsistentTrue;
    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> descriptionUpdated;
    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> depAdded;
    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> bDepAdded;
    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> depRemoved;
    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> bDepRemoved;
    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> nodeRemoved;
    @Setter(AccessLevel.PACKAGE)
    private ParentChangedListener parentChanged;
    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> nodeNameUpdated;

    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, NodeGenericMetadata> nodeMetadataUpdated;

    @Autowired
    NodeService(NodeRepository nodeRepository,
                MetaDataService metaDataService,
                DependencyRepository dependencyRepository) {
        this.nodeRepository = Objects.requireNonNull(nodeRepository);
        this.metaDataService = Objects.requireNonNull(metaDataService);
        depRepository = Objects.requireNonNull(dependencyRepository);
    }

//    void setNodeMetadataUpdate(BiConsumer<String, NodeGenericMetadata> metadataUpdate) {
//        metaDataService.setNodeMetadataUpdated(metadataUpdate);
//    }

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
        nodeRepository.updateDescriptionById(nodeId, description);
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
        final String id = getParentNode(nodeId).get().getId();
        System.out.println(id);
        nodeRepository.updateParentById(nodeId, newParentNodeId);
        parentChanged.consume(nodeId, id, newParentNodeId);
    }

    String deleteNode(String nodeId) {
        final NodeInfoEntity nodeToDelete = nodeRepository.findById(nodeId).get();
        cleanDep(nodeToDelete);
        final Optional<NodeInfo> parentNode = getParentNode(nodeId);
        final String parentId = parentNode.map(NodeInfo::getId).orElse(null);
        final List<NodeInfo> childNodes = getChildNodes(nodeId);
        childNodes.forEach(id -> deleteNode(id.getId()));
        metaDataService.deleteMetaDate(nodeId);
        nodeRepository.deleteById(nodeToDelete.getId());
        nodeRemoved.accept(nodeId, parentId);
        return parentId;
    }

    private void cleanDep(NodeInfoEntity nodeToDelete) {
        depRepository.findAllByFrom(nodeToDelete)
                .forEach(de -> {
                    depRemoved.accept(nodeToDelete.getId(), de.getName());
                    bDepRemoved.accept(de.getTo().getId(), de.getName());
                });
        depRepository.deleteByFrom(nodeToDelete);
        depRepository.deleteByTo(nodeToDelete);
        // TODO ???
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

    Set<NodeDependency> getDependencies(String nodeId) {
        final Optional<NodeInfoEntity> from = nodeRepository.findById(nodeId);
        return depRepository.findAllByFrom(from.get()).stream()
                .map(de -> new NodeDependency(de.getName(), getNodeInfo(de.getTo().getId())))
                .collect(Collectors.toSet());
    }

    Set<NodeInfo> getDependencies(String nodeId, String name) {
        final Optional<NodeInfoEntity> from = nodeRepository.findById(nodeId);
        return depRepository.findAllByFromAndName(from.get(), name)
                .stream().map(DependencyEntity::getTo)
                // TODO ??? no need to re get if metadata is bounded already to entity
                .map(NodeInfoEntity::getId)
                .map(this::getNodeInfo)
                .collect(Collectors.toSet());
    }

    void addDependency(String nodeId, String name, String toNodeId) {
        final Optional<NodeInfoEntity> from = nodeRepository.findById(nodeId);
        final Optional<NodeInfoEntity> to = nodeRepository.findById(toNodeId);
        final DependencyEntity dependencyEntity = new DependencyEntity()
                .setFrom(from.get())
                .setTo(to.get())
                .setName(name);
        depRepository.save(dependencyEntity);
        depAdded.accept(nodeId, name);
        bDepAdded.accept(toNodeId, name);
    }

    Set<NodeInfo> getBackwardDependencies(String nodeId) {
        final Optional<NodeInfoEntity> toId = nodeRepository.findById(nodeId);
        return depRepository.findAllByTo(toId.get())
                .stream().map(DependencyEntity::getFrom)
                // TODO ??? no need to re get if metadata is bounded already to entity
                .map(NodeInfoEntity::getId)
                .map(this::getNodeInfo)
                .collect(Collectors.toSet());
    }

    void removeDependency(String nodeId, String name, String toNodeId) {
        depRepository.deleteByFromAndNameAndTo(nodeRepository.findById(nodeId).get(), name, nodeRepository.findById(toNodeId).get());
        depRemoved.accept(nodeId, name);
        bDepRemoved.accept(toNodeId, name);
    }

    void renameNode(String nodeId, String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Impossible to rename node '" + nodeId + "' with an empty or null name");
        }
        nodeRepository.updateNameById(nodeId, name);
        nodeNameUpdated.accept(nodeId, name);
    }

    void setMetadata(String nodeId, NodeGenericMetadata genericMetadata) {
        metaDataService.saveMetaData(nodeId, genericMetadata);
        nodeMetadataUpdated.accept(nodeId, genericMetadata);
    }
}
