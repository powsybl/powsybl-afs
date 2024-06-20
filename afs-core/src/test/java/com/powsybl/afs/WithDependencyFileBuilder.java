package com.powsybl.afs;

import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;

public class WithDependencyFileBuilder implements ProjectFileBuilder<WithDependencyFile> {

    private final ProjectFileBuildContext context;

    private String name;

    WithDependencyFileBuilder(ProjectFileBuildContext context) {
        this.context = context;
    }

    public WithDependencyFileBuilder withName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public WithDependencyFile build() {
        // check parameters
        if (name == null) {
            throw new AfsException("Name is not set");
        }
        NodeInfo info = context.getStorage().createNode(context.getFolderInfo().getId(), name, "WITH_DEPENDENCY_FILE", "", 0, new NodeGenericMetadata());
        context.getStorage().setConsistent(info.getId());
        return new WithDependencyFile(new ProjectFileCreationContext(info, context.getStorage(), context.getProject()));
    }
}
