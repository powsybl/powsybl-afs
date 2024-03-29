/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.events.*;
import com.powsybl.commons.util.WeakListenerList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class ProjectFile extends ProjectNode {

    private final WeakListenerList<ProjectFileListener> listeners = new WeakListenerList<>();

    private final AppStorageListener l = eventList -> {
        for (NodeEvent event : eventList.getEvents()) {
            if (event.getId().equals(getId())) {
                switch (event.getType()) {
                    case DependencyAdded.TYPENAME, DependencyRemoved.TYPENAME -> listeners.notify(listener -> listener.dependencyChanged(((DependencyEvent) event).getDependencyName()));
                    case BackwardDependencyAdded.TYPENAME, BackwardDependencyRemoved.TYPENAME -> listeners.notify(listener -> listener.backwardDependencyChanged(((DependencyEvent) event).getDependencyName()));
                    default -> {
                        // Do nothing
                    }
                }
            }
        }
    };

    protected ProjectFile(ProjectFileCreationContext context, int codeVersion) {
        super(context, codeVersion, true);
        if (context.isConnected()) {
            storage.getEventsBus().addListener(l);
        }
    }

    @Override
    public boolean isFolder() {
        return false;
    }

    public List<ProjectDependency<ProjectNode>> getDependencies() {
        return getDependencies(false);
    }

    public List<ProjectDependency<ProjectNode>> getDependencies(boolean connected) {
        return storage.getDependencies(info.getId())
                .stream()
                .map(dependency -> new ProjectDependency<>(dependency.getName(), project.createProjectNode(dependency.getNodeInfo(), connected)))
                .collect(Collectors.toList());
    }

    public void setDependencies(String name, List<ProjectNode> projectNodes) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(projectNodes);
        for (NodeInfo toNodeInfo : storage.getDependencies(info.getId(), name)) {
            storage.removeDependency(info.getId(), name, toNodeInfo.getId());
        }
        for (ProjectNode projectNode : projectNodes) {
            storage.addDependency(info.getId(), name, projectNode.getId());
        }
        storage.flush();
    }

    public void replaceDependency(String oldDependencyId, ProjectNode replacementNode) {
        Objects.requireNonNull(oldDependencyId);
        Objects.requireNonNull(replacementNode);
        Map<String, List<ProjectDependency<ProjectNode>>> dependencies = getDependencies().stream().collect(Collectors.groupingBy(ProjectDependency::getName));
        dependencies.forEach((depKey, depValue) -> setDependencies(depKey, depValue.stream().map(projectNodeProjectDependency -> {
            if (projectNodeProjectDependency.getProjectNode().getId().equals(oldDependencyId)) {
                return replacementNode;
            } else {
                return projectNodeProjectDependency.getProjectNode();
            }
        }).collect(Collectors.toList())));
    }

    public <T extends ProjectNode> List<T> getDependencies(String name, Class<T> nodeClass) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(nodeClass);
        return storage.getDependencies(info.getId(), name).stream()
                .map(nodeInfo -> project.createProjectNode(nodeInfo, false))
                .filter(dependencyNode -> nodeClass.isAssignableFrom(dependencyNode.getClass()))
                .map(nodeClass::cast)
                .collect(Collectors.toList());
    }

    public void removeDependencies(String name) {
        Objects.requireNonNull(name);
        for (NodeInfo toNodeInfo : storage.getDependencies(info.getId(), name)) {
            storage.removeDependency(info.getId(), name, toNodeInfo.getId());
        }
        storage.flush();
    }

    public boolean hasDeepDependency(ProjectFile candidateDependency) {
        return hasDeepDependency(candidateDependency, null);
    }

    public boolean hasDeepDependency(ProjectFile candidateDependency, String dependencyName) {
        List<ProjectFile> dependencies = (dependencyName != null) ?
                getDependencies(dependencyName, ProjectFile.class) :
                getDependencies().stream().map(ProjectDependency::getProjectNode).filter(dep -> ProjectFile.class.isAssignableFrom(dep.getClass())).map(ProjectFile.class::cast).collect(Collectors.toList());
        return dependencies.stream().anyMatch(dep -> dep.getId().equals(candidateDependency.getId()) || dep.hasDeepDependency(candidateDependency, dependencyName));
    }

    public boolean mandatoryDependenciesAreMissing() {
        return false;
    }

    public void addListener(ProjectFileListener listener) {
        listeners.add(listener);
    }

    public void removeListener(ProjectFileListener listener) {
        listeners.remove(listener);
    }

    public UUID startTask() {
        return project.getFileSystem().getTaskMonitor().startTask(this).getId();
    }

    public AppLogger createLogger(UUID taskId) {
        return new TaskMonitorLogger(project.getFileSystem().getTaskMonitor(), taskId);
    }

    public void stopTask(UUID id) {
        project.getFileSystem().getTaskMonitor().stopTask(id);
    }

}
