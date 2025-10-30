/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Paul Bui-Quang {@literal <paul.buiquang at rte-france.com>}
 */
public class OrderedDependencyManager {

    private final ProjectFile projectFile;

    private List<ProjectDependency<ProjectNode>> dependencyCache = null;

    public OrderedDependencyManager(ProjectFile projectFile) {
        this.projectFile = Objects.requireNonNull(projectFile);
        projectFile.addListener(new DefaultProjectFileListener() {
            @Override
            public void dependencyChanged(String name) {
                dependencyCache = null;
            }
        });
    }

    public void appendDependencies(String name, List<ProjectNode> projectNodes) {
        List<ProjectNode> nodes = getDependencies(name);
        nodes.addAll(projectNodes);
        setDependencies(name, nodes);
    }

    public void insertDependencies(String name, int index, List<ProjectNode> projectNodes) {
        List<ProjectNode> nodes = getDependencies(name);
        nodes.addAll(index, projectNodes);
        setDependencies(name, nodes);
    }

    public void removeDependency(String name, int index) {
        List<ProjectNode> nodes = getDependencies(name);
        nodes.remove(index);
        setDependencies(name, nodes);
    }

    public void removeDependencies(String name, List<String> nodeIds) {
        List<ProjectNode> nodes = getDependencies(name)
                .stream()
                .filter(dep -> !nodeIds.contains(dep.getId()))
                .collect(Collectors.toList());
        setDependencies(name, nodes);
    }

    /**
     *
     * @param name
     */
    public void removeAllDependencies(String name) {
        setDependencies(name, Collections.emptyList());
    }

    public void setDependencies(String name, List<ProjectNode> projectNodes) {
        getDependenciesFor(name).map(el -> el.getRight().getName()).forEach(projectFile::removeDependencies);
        for (int i = 0; i < projectNodes.size(); i++) {
            projectFile.setDependencies(name + "_" + i, Collections.singletonList(projectNodes.get(i)));
        }
        clearCache();
    }

    public List<ProjectNode> getDependencies(String name) {
        return getDependenciesFor(name)
                .sorted(Comparator.comparing(ImmutablePair::getLeft))
                .map(dep -> dep.getRight().getProjectNode())
                .collect(Collectors.toList());
    }

    public <T extends ProjectNode> List<T> getDependencies(String name, Class<T> nodeClass) {
        return getDependenciesFor(name)
                .filter(dep -> nodeClass.isAssignableFrom(dep.getRight().getProjectNode().getClass()))
                .sorted(Comparator.comparing(ImmutablePair::getLeft))
                .map(dep -> nodeClass.cast(dep.getRight().getProjectNode()))
                .collect(Collectors.toList());
    }

    private List<ProjectDependency<ProjectNode>> getDependencyCache() {
        if (dependencyCache == null) {
            dependencyCache = projectFile.getDependencies(true);
        }
        return dependencyCache;
    }

    private Stream<ImmutablePair<String, ProjectDependency<ProjectNode>>> getDependenciesFor(String name) {
        Pattern pattern = Pattern.compile(name + "_(\\d+)");
        return getDependencyCache()
                .stream()
                .map(dep -> {
                    Matcher depMatch = pattern.matcher(dep.getName());
                    if (depMatch.matches()) {
                        return ImmutablePair.of(depMatch.group(1), dep);
                    }
                    return null;
                })
                .filter(Objects::nonNull);
    }

    public void clearCache() {
        dependencyCache = null;
    }

}
