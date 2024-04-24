package com.powsybl.afs;

import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;

public class WithDependencyFileBuilder implements ProjectFileBuilder<WithDependencyFile> {

    private final ProjectFileBuildContext context;

    WithDependencyFileBuilder(ProjectFileBuildContext context) {
        this.context = context;
    }

    @Override
    public WithDependencyFile build() {
        NodeInfo info = context.getStorage().createNode(context.getFolderInfo().getId(), "WithDependencyFile", "WITH_DEPENDENCY_FILE", "", 0, new NodeGenericMetadata());
        context.getStorage().setConsistent(info.getId());
        return new WithDependencyFile(new ProjectFileCreationContext(info, context.getStorage(), context.getProject()));
    }
}
