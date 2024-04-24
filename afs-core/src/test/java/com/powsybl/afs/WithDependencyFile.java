package com.powsybl.afs;

import com.powsybl.afs.storage.events.NodeEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WithDependencyFile extends ProjectFile {

    private static final String DEP_NAME = "dep";

    private final DependencyCache<ProjectFile> cache = new DependencyCache<>(this, DEP_NAME, ProjectFile.class);
    final List<NodeEvent> events = new ArrayList<>();

    WithDependencyFile(ProjectFileCreationContext context) {
        super(context, 0);
        if (context.isConnected()) {
            context.getStorage().getEventsBus().addListener(eventList -> events.addAll(eventList.getEvents()));
        }
    }

    ProjectFile getTicDependency() {
        return cache.getFirst().orElse(null);
    }

    void setFooDependency(FooFile foo) {
        setDependencies(DEP_NAME, Collections.singletonList(foo));
        cache.invalidate();
    }
}
