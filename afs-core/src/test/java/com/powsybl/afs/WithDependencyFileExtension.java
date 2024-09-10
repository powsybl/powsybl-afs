package com.powsybl.afs;

public class WithDependencyFileExtension implements ProjectFileExtension<WithDependencyFile, WithDependencyFileBuilder> {

    @Override
    public Class<WithDependencyFile> getProjectFileClass() {
        return WithDependencyFile.class;
    }

    @Override
    public String getProjectFilePseudoClass() {
        return "WITH_DEPENDENCY_FILE";
    }

    @Override
    public Class<WithDependencyFileBuilder> getProjectFileBuilderClass() {
        return WithDependencyFileBuilder.class;
    }

    @Override
    public WithDependencyFile createProjectFile(ProjectFileCreationContext context) {
        return new WithDependencyFile(context);
    }

    @Override
    public ProjectFileBuilder<WithDependencyFile> createProjectFileBuilder(ProjectFileBuildContext context) {
        return new WithDependencyFileBuilder(context);
    }
}
