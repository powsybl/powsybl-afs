/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.google.auto.service.AutoService;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.commons.util.ServiceLoaderCache;
import com.powsybl.tools.Command;
import com.powsybl.tools.CommandLineTools;
import com.powsybl.tools.Tool;
import com.powsybl.tools.ToolRunningContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@AutoService(Tool.class)
public class AppFileSystemTool implements Tool {

    private static final String LS = "ls";
    private static final String ARCHIVE = "archive";
    private static final String UNARCHIVE = "unarchive";
    private static final String ZIP = "zip";
    private static final String DEPENDENCIES = "dependencies";
    private static final String DELETE_RESULT_OPTNAME = "deleteResults";
    private static final String DIR = "dir";
    private static final String LS_INCONSISTENT_NODES = "ls-inconsistent-nodes";
    private static final String FIX_INCONSISTENT_NODES = "fix-inconsistent-nodes";
    private static final String RM_INCONSISTENT_NODES = "rm-inconsistent-nodes";
    private static final String FILE_SYSTEM_NAME = "FILE_SYSTEM_NAME";
    private static final String FILE_SYSTEM = "File system'";
    private static final String NOT_FOUND = "not found'";

    private static final ServiceLoaderCache<ProjectFileExtension> PROJECT_FILE_EXECUTION = new ServiceLoaderCache<>(ProjectFileExtension.class);

    private static InconsistentNodeParam getInconsistentNodeParam(CommandLine line, String optionName) {
        String[] values = line.getOptionValues(optionName);
        if (values == null) {
            throw new IllegalArgumentException("Invalid values for option: " + optionName);
        }
        InconsistentNodeParam param = new InconsistentNodeParam();
        if (values.length == 2) {
            param.fileSystemName = values[0];
            param.nodeId = values[1];
        } else if (values.length == 1) {
            param.fileSystemName = values[0];
        }
        return param;
    }

    protected AppData createAppData(ToolRunningContext context) {
        return new AppData(context.getShortTimeExecutionComputationManager(),
            context.getLongTimeExecutionComputationManager());
    }

    @Override
    public Command getCommand() {
        return new Command() {
            @Override
            public String getName() {
                return "afs";
            }

            @Override
            public String getTheme() {
                return "Application file system";
            }

            @Override
            public String getDescription() {
                return "application file system command line tool";
            }

            @Override
            public Options getOptions() {
                Options options = new Options();
                OptionGroup topLevelOptions = new OptionGroup();
                topLevelOptions.addOption(Option.builder()
                    .longOpt(LS)
                    .desc("list files")
                    .hasArg()
                    .optionalArg(true)
                    .argName("PATH")
                    .build());
                topLevelOptions.addOption(Option.builder()
                    .longOpt(ARCHIVE)
                    .desc("archive file system")
                    .hasArg()
                    .optionalArg(true)
                    .argName(FILE_SYSTEM_NAME)
                    .build());
                topLevelOptions.addOption(Option.builder()
                    .longOpt(UNARCHIVE)
                    .desc("unarchive file system")
                    .hasArg()
                    .optionalArg(true)
                    .argName(FILE_SYSTEM_NAME)
                    .build());
                topLevelOptions.addOption(Option.builder()
                    .longOpt(LS_INCONSISTENT_NODES)
                    .desc("list the inconsistent nodes")
                    .hasArg()
                    .optionalArg(true)
                    .argName(FILE_SYSTEM_NAME)
                    .build());
                topLevelOptions.addOption(Option.builder()
                    .longOpt(FIX_INCONSISTENT_NODES)
                    .desc("make inconsistent nodes consistent")
                    .optionalArg(true)
                    .argName(FILE_SYSTEM_NAME + "> <NODE_ID")
                    .numberOfArgs(2)
                    .valueSeparator(',')
                    .build());
                topLevelOptions.addOption(Option.builder()
                    .longOpt(RM_INCONSISTENT_NODES)
                    .desc("remove inconsistent nodes")
                    .hasArg()
                    .optionalArg(true)
                    .argName(FILE_SYSTEM_NAME + "> <NODE_ID")
                    .numberOfArgs(2)
                    .valueSeparator(',')
                    .build());
                options.addOptionGroup(topLevelOptions);
                options.addOption(Option.builder()
                    .longOpt(DELETE_RESULT_OPTNAME)
                    .desc("delete results")
                    .hasArg(false)
                    .build());
                options.addOption(Option.builder()
                    .longOpt(DIR)
                    .desc("directory")
                    .hasArg()
                    .argName("DIR")
                    .build());
                options.addOption(Option.builder()
                    .longOpt(DEPENDENCIES)
                    .desc("archive dependencies")
                    .hasArg(false)
                    .build());
                options.addOption(Option.builder()
                    .longOpt(ZIP)
                    .desc("zip file system")
                    .hasArg(false)
                    .build());
                return options;
            }

            @Override
            public String getUsageFooter() {
                return null;
            }
        };
    }

    private void runLs(CommandLine line, ToolRunningContext context) {
        try (AppData appData = createAppData(context)) {
            String path = line.getOptionValue(LS);
            if (path == null) {
                for (AppFileSystem afs : appData.getFileSystems()) {
                    context.getOutputStream().println(afs.getName());
                }
            } else {
                Optional<Node> node = appData.getNode(path);
                if (node.isPresent()) {
                    if (node.get().isFolder()) {
                        ((Folder) node.get()).getChildren().forEach(child -> context.getOutputStream().println(child.getName()));
                    } else {
                        context.getErrorStream().println("'" + path + "' is not a folder");
                    }
                } else {
                    context.getErrorStream().println("'" + path + "' does not exist");
                }
            }
        }
    }

    private AppFileSystem getAppFileSystem(CommandLine line, AppData appData, String optionValue) {
        String fileSystemName = line.getOptionValue(optionValue);
        AppFileSystem fs = appData.getFileSystem(fileSystemName);
        if (fs == null) {
            throw new AfsException("File system '" + fileSystemName + "' not found");
        }
        return fs;
    }

    private void runUnarchive(CommandLine line, ToolRunningContext context) {
        if (!line.hasOption(DIR)) {
            throw new AfsException("dir option is missing");
        }
        try (AppData appData = createAppData(context)) {
            AppFileSystem fs = getAppFileSystem(line, appData, UNARCHIVE);
            Path dir = context.getFileSystem().getPath(line.getOptionValue(DIR));
            boolean mustZip = line.hasOption(ZIP);
            fs.getRootFolder().unarchive(dir, mustZip);
        }
    }

    private void runArchive(CommandLine line, ToolRunningContext context) {
        if (!line.hasOption(DIR)) {
            throw new AfsException("dir option is missing");
        }
        try (AppData appData = createAppData(context)) {
            AppFileSystem fs = getAppFileSystem(line, appData, ARCHIVE);
            Path dir = context.getFileSystem().getPath(line.getOptionValue(DIR));
            boolean mustZip = line.hasOption(ZIP);
            boolean archiveDependencies = line.hasOption(DEPENDENCIES);
            boolean deleteResult = line.hasOption(DELETE_RESULT_OPTNAME);
            Map<String, List<String>> outputBlackList = new HashMap<>();
            List<String> keepTs = new ArrayList<>();
            if (deleteResult) {
                outputBlackList = PROJECT_FILE_EXECUTION.getServices().stream()
                    .collect(Collectors.toMap(ProjectFileExtension::getProjectFilePseudoClass,
                        ProjectFileExtension::getOutputList));
                keepTs = PROJECT_FILE_EXECUTION.getServices().stream().filter(ProjectFileExtension::removeTSWhenArchive).map(ProjectFileExtension::getProjectFilePseudoClass)
                    .collect(Collectors.toList());
            }
            fs.getRootFolder().archive(dir, mustZip, archiveDependencies, outputBlackList, keepTs);
        }
    }

    private void lsInconsistentNodes(ToolRunningContext context, AppFileSystem afs, String name) {
        List<NodeInfo> nodeInfos = afs.getStorage().getInconsistentNodes();
        if (!nodeInfos.isEmpty()) {
            context.getOutputStream().println(name + ":");
            nodeInfos.forEach(nodeInfo -> context.getOutputStream().println(nodeInfo.getId()));
        }
    }

    private void runLsInconsistentNodes(CommandLine line, ToolRunningContext context) {
        try (AppData appData = createAppData(context)) {
            String fileSystemName = line.getOptionValue(LS_INCONSISTENT_NODES);
            if (fileSystemName == null) {
                for (AppFileSystem afs : appData.getFileSystems()) {
                    lsInconsistentNodes(context, afs, afs.getName());
                }
            } else {
                AppFileSystem fs = appData.getFileSystem(fileSystemName);
                if (fs == null) {
                    throw new AfsException(FILE_SYSTEM + fileSystemName + NOT_FOUND);
                }
                lsInconsistentNodes(context, fs, fileSystemName);
            }
        }
    }

    private void fixInconsistentNodes(ToolRunningContext context, AppFileSystem fs, String nodeId) {
        List<NodeInfo> nodeInfos = fs.getStorage().getInconsistentNodes();
        if (!nodeInfos.isEmpty()) {
            context.getOutputStream().println(fs.getName() + ":");
            if (nodeId == null) {
                nodeInfos.forEach(nodeInfo -> {
                    fs.getStorage().setConsistent(nodeInfo.getId());
                    context.getOutputStream().println(nodeInfo.getId() + " fixed");
                });
            } else {
                nodeInfos.stream()
                    .filter(nodeInfo -> nodeId.equals(nodeInfo.getId()))
                    .forEach(nodeInfo -> {
                        fs.getStorage().setConsistent(nodeInfo.getId());
                        context.getOutputStream().println(nodeInfo.getId() + " fixed");
                    });
            }
        }
    }

    private void runFixInconsistentNodes(CommandLine line, ToolRunningContext context) {
        try (AppData appData = createAppData(context)) {
            InconsistentNodeParam param = getInconsistentNodeParam(line, FIX_INCONSISTENT_NODES);
            if (param.fileSystemName == null) {
                for (AppFileSystem afs : appData.getFileSystems()) {
                    fixInconsistentNodes(context, afs, param.nodeId);
                }
            } else {
                AppFileSystem afs = appData.getFileSystem(param.fileSystemName);
                if (afs == null) {
                    throw new AfsException(FILE_SYSTEM + param.fileSystemName + NOT_FOUND);
                }
                fixInconsistentNodes(context, afs, param.nodeId);
            }
        }
    }

    private void removeInconsistentNodes(ToolRunningContext context, AppFileSystem fs, String nodeId) {
        List<NodeInfo> nodeInfos = fs.getStorage().getInconsistentNodes();
        if (!nodeInfos.isEmpty()) {
            context.getOutputStream().println(fs.getName() + ":");
            if (nodeId == null) {
                nodeInfos.forEach(nodeInfo -> {
                    fs.getStorage().deleteNode(nodeInfo.getId());
                    context.getOutputStream().println(nodeInfo.getId() + " cleaned");
                });
            } else {
                nodeInfos.stream()
                    .filter(nodeInfo -> nodeId.equals(nodeInfo.getId()))
                    .forEach(nodeInfo -> {
                        fs.getStorage().deleteNode(nodeInfo.getId());
                        context.getOutputStream().println(nodeInfo.getId() + " cleaned");
                    });
            }
        }
    }

    private void runRemoveInconsistentNodes(CommandLine line, ToolRunningContext context) {
        try (AppData appData = createAppData(context)) {
            InconsistentNodeParam param = getInconsistentNodeParam(line, RM_INCONSISTENT_NODES);
            if (param.fileSystemName == null) {
                for (AppFileSystem afs : appData.getFileSystems()) {
                    removeInconsistentNodes(context, afs, param.nodeId);
                }
            } else {
                AppFileSystem afs = appData.getFileSystem(param.fileSystemName);
                if (afs == null) {
                    throw new AfsException(FILE_SYSTEM + param.fileSystemName + NOT_FOUND);
                }
                removeInconsistentNodes(context, afs, param.nodeId);
            }
        }
    }

    @Override
    public void run(CommandLine line, ToolRunningContext context) throws IOException {
        if (line.hasOption(LS)) {
            runLs(line, context);
        } else if (line.hasOption(ARCHIVE)) {
            runArchive(line, context);
        } else if (line.hasOption(UNARCHIVE)) {
            runUnarchive(line, context);
        } else if (line.hasOption(LS_INCONSISTENT_NODES)) {
            runLsInconsistentNodes(line, context);
        } else if (line.hasOption(FIX_INCONSISTENT_NODES)) {
            runFixInconsistentNodes(line, context);
        } else if (line.hasOption(RM_INCONSISTENT_NODES)) {
            runRemoveInconsistentNodes(line, context);
        } else {
            Command command = getCommand();
            CommandLineTools.printCommandUsage(command.getName(), command.getOptions(), command.getUsageFooter(), context.getErrorStream());
        }
    }

    static class InconsistentNodeParam {
        String fileSystemName;
        String nodeId;
    }
}
