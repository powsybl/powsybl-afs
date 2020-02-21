/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.io.ByteStreams;
import com.powsybl.afs.storage.json.AppStorageJsonModule;
import com.powsybl.commons.json.JsonUtil;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class AppStorageArchive {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppStorageArchive.class);

    private static final Pattern CHUNKS_PATTERN = Pattern.compile("chunks-(\\d*).json.gz");

    private static class ArchiveDependency {

        @JsonProperty("nodeId")
        private final String nodeId;

        @JsonProperty("name")
        private final String name;

        @JsonCreator
        public ArchiveDependency(@JsonProperty("nodeId") String nodeId, @JsonProperty("name") String name) {
            this.nodeId = Objects.requireNonNull(nodeId);
            this.name = Objects.requireNonNull(name);
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "Dependency(nodeId=" + nodeId + ", name=" + name  + ")";
        }
    }

    private static class UnarchiveContext {

        private final Map<String, String> idMapping = new HashMap<>();

        private final Map<String, List<ArchiveDependency>> dependencies = new HashMap<>();

        public Map<String, String> getIdMapping() {
            return idMapping;
        }

        public String getIdMapping(String nodeId) {
            // if node id belongs to the archive, map dependency to new id otherwise map to old id because
            String newToNodeId = idMapping.get(nodeId);
            if (newToNodeId != null) {
                return newToNodeId;
            }
            return nodeId;
        }

        public Map<String, List<ArchiveDependency>> getDependencies() {
            return dependencies;
        }
    }

    private static class ArchiveContext {

        private final List<String> idListObject = new ArrayList<>();

        private final boolean archiveDependencies;

        private final boolean keepResults;

        public ArchiveContext(boolean archiveDependencies, boolean keepResults) {
            this.archiveDependencies = archiveDependencies;
            this.keepResults = keepResults;
        }

        public List<String> getIdListObject() {
            return idListObject;
        }

        public boolean isArchiveDependencies() {
            return archiveDependencies;
        }

        public boolean isKeepResults() {
            return keepResults;
        }
    }

    private final AppStorage storage;

    private final ObjectMapper mapper;

    private final ObjectWriter objectWriter;

    public AppStorageArchive(AppStorage storage) {
        this.storage = Objects.requireNonNull(storage);
        mapper = JsonUtil.createObjectMapper()
                .registerModule(new AppStorageJsonModule());
        objectWriter = mapper.writerWithDefaultPrettyPrinter();
    }

    private void writeNodeInfo(NodeInfo nodeInfo, Path nodeDir) throws IOException {
        try (Writer writer = Files.newBufferedWriter(nodeDir.resolve("info.json"), StandardCharsets.UTF_8)) {
            objectWriter.writeValue(writer, nodeInfo);
        }
    }

    private void writeDependencies(NodeInfo nodeInfo, Path nodeDir) throws IOException {
        List<ArchiveDependency> dependencies = storage.getDependencies(nodeInfo.getId())
                .stream()
                .map(nodeDependency -> new ArchiveDependency(nodeDependency.getNodeInfo().getId(), nodeDependency.getName()))
                .collect(Collectors.toList());
        if (dependencies.isEmpty()) {
            return;
        }
        try (Writer writer = Files.newBufferedWriter(nodeDir.resolve("dependencies.json"), StandardCharsets.UTF_8)) {
            objectWriter.writeValue(writer, dependencies);
        }
    }

    private void writeData(NodeInfo nodeInfo, Path nodeDir, ArchiveContext archiveDependencies)  throws IOException {

        Set<String> dataNames = storage.getDataNames(nodeInfo.getId());
        if (dataNames.isEmpty()) {
            return;
        }

        LOGGER.info("   Writing {} data", dataNames.size());

        Path dataDir = nodeDir.resolve("data");
        Files.createDirectory(dataDir);

        for (String dataName : dataNames) {
            if (archiveDependencies.isKeepResults() || dataName.equals("parameters") || dataName.equals("scriptContent")) {
                Path dataFileName = dataDir.resolve(URLEncoder.encode(dataName, StandardCharsets.UTF_8.name()) + ".gz");
                try (InputStream is = storage.readBinaryData(nodeInfo.getId(), dataName).orElseThrow(AssertionError::new);
                     OutputStream os = new GZIPOutputStream(Files.newOutputStream(dataFileName))) {
                    ByteStreams.copy(is, os);
                }
            }
        }
    }

    private void writeTimeSeries(NodeInfo nodeInfo, Path nodeDir) throws IOException {
        Set<String> timeSeriesNames = storage.getTimeSeriesNames(nodeInfo.getId());
        if (timeSeriesNames.isEmpty()) {
            return;
        }

        LOGGER.info("   Writing {} time series", timeSeriesNames.size());

        Path timeSeriesDir = nodeDir.resolve("time-series");
        Files.createDirectory(timeSeriesDir);

        for (TimeSeriesMetadata metadata : storage.getTimeSeriesMetadata(nodeInfo.getId(), timeSeriesNames)) {
            String timeSeriesFileName = URLEncoder.encode(metadata.getName(), StandardCharsets.UTF_8.name());
            Path timeSeriesNameDir = timeSeriesDir.resolve(timeSeriesFileName);
            Files.createDirectory(timeSeriesNameDir);

            // write metadata
            try (Writer writer = Files.newBufferedWriter(timeSeriesNameDir.resolve("metadata.json"), StandardCharsets.UTF_8)) {
                objectWriter.writeValue(writer, metadata);
            }

            // write chunks for each version
            for (int version : storage.getTimeSeriesDataVersions(nodeInfo.getId(), metadata.getName())) {
                switch (metadata.getDataType()) {
                    case DOUBLE:
                        List<DoubleDataChunk> doubleChunks = storage.getDoubleTimeSeriesData(nodeInfo.getId(), Collections.singleton(metadata.getName()), version).get(metadata.getName());
                        try (Writer writer = new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(timeSeriesNameDir.resolve("chunks-" + version + ".json.gz"))), StandardCharsets.UTF_8)) {
                            objectWriter.writeValue(writer, doubleChunks);
                        }
                        break;

                    case STRING:
                        List<StringDataChunk> stringChunks = storage.getStringTimeSeriesData(nodeInfo.getId(), Collections.singleton(metadata.getName()), version).get(metadata.getName());
                        try (Writer writer = new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(timeSeriesNameDir.resolve("chunks-" + version + ".json.gz"))), StandardCharsets.UTF_8)) {
                            objectWriter.writeValue(writer, stringChunks);
                        }
                        break;

                    default:
                        throw new AssertionError("Unsupported data type " + metadata.getDataType());
                }
            }
        }
    }

    public void archiveChildren(NodeInfo nodeInfo, Path nodeDir) throws IOException {
        archiveChildren(nodeInfo, nodeDir, new ArchiveContext(false, true));
    }

    public void archiveChildren(NodeInfo nodeInfo, Path nodeDir, ArchiveContext archiveDependencies) throws IOException {
        Objects.requireNonNull(nodeInfo);
        Objects.requireNonNull(nodeDir);
        List<NodeInfo> childNodeInfos = storage.getChildNodes(nodeInfo.getId());
        if (!childNodeInfos.isEmpty()) {
            Path childrenDir = nodeDir.resolve("children");
            Files.createDirectory(childrenDir);
            for (NodeInfo childNodeInfo : childNodeInfos) {
                archive(childNodeInfo, childrenDir, archiveDependencies);
            }
        }
    }

    private void archiveDependencies(NodeInfo nodeInfo, Path nodeDir, ArchiveContext archiveDependencies) throws IOException {
        Objects.requireNonNull(nodeInfo);
        Objects.requireNonNull(nodeDir);
        Set<NodeDependency> dependenciesNodes = storage.getDependencies(nodeInfo.getId());
        if (!dependenciesNodes.isEmpty()) {
            Path dependenciesDir = nodeDir.resolve("dependencies");
            Files.createDirectory(dependenciesDir);
            for (NodeDependency dependenciesNode : dependenciesNodes) {
                archive(dependenciesNode.getNodeInfo(), dependenciesDir, archiveDependencies);
            }
        }
    }

    public void archive(String nodeId, Path parentDir) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(parentDir);

        try {
            archive(storage.getNodeInfo(nodeId), parentDir, new ArchiveContext(false, true));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void archive(String nodeId, Path parentDir, boolean archiveDependencies, boolean keepResults) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(parentDir);

        try {
            archive(storage.getNodeInfo(nodeId), parentDir, new ArchiveContext(archiveDependencies, keepResults));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void archive(NodeInfo nodeInfo, Path parentDir, ArchiveContext archiveDependencies) throws IOException {
        Objects.requireNonNull(nodeInfo);
        Path directory = Objects.requireNonNull(parentDir);

        if (!archiveDependencies.getIdListObject().contains(nodeInfo.getId())) {

            LOGGER.info("Archiving node {} ({})", nodeInfo.getId(), nodeInfo.getName());

            archiveDependencies.getIdListObject().add(nodeInfo.getId());

            // If it is a dependency, we will search for these parents and
            // save them if they are different from the folder that we archive
            if (parentDir.getFileName().toString().equals("dependencies")) {
                directory = archiveParent(nodeInfo, parentDir);
            }

            Path nodeDir = directory.resolve(nodeInfo.getId());
            Files.createDirectory(nodeDir);

            writeNodeInfo(nodeInfo, nodeDir);

            writeDependencies(nodeInfo, nodeDir);

            writeData(nodeInfo, nodeDir, archiveDependencies);

            writeTimeSeries(nodeInfo, nodeDir);

            archiveChildren(nodeInfo, nodeDir, archiveDependencies);

            if (archiveDependencies.isArchiveDependencies()) {
                archiveDependencies(nodeInfo, nodeDir, archiveDependencies);
            }
        }
    }

    /**
     * archive folder
     * @param nodeInfo node's information
     * @param parentDir directory where archive
     * @throws IOException Exception if the directory can't be created
     */
    private void archiveFolder(NodeInfo nodeInfo, Path parentDir) throws IOException {
        Objects.requireNonNull(nodeInfo);
        Objects.requireNonNull(parentDir);
        LOGGER.info("Archiving node {} ({})", nodeInfo.getId(), nodeInfo.getName());
        Path nodeDir = parentDir.resolve(nodeInfo.getId());
        if (!Files.exists(nodeDir)) {
            Files.createDirectory(nodeDir);
            writeNodeInfo(nodeInfo, nodeDir);
            Path childrenDir = nodeDir.resolve("children");
            Files.createDirectory(childrenDir);
        }
    }

    /**
     * archive all parent's node of a node
     * @param nodeInfo
     * @param parentNodeDir
     * @return
     */
    private Path archiveParent(NodeInfo nodeInfo, Path parentNodeDir) {
        Optional<NodeInfo> parentNodeInfo = storage.getParentNode(nodeInfo.getId());
        AtomicReference<Path> nodePath = new AtomicReference<>();
        parentNodeInfo.ifPresent(node -> {
            if (!"root".equals(node.getName()) && !parentNodeDir.toString().contains(node.getId())) {
                try {
                    nodePath.set(archiveParent(node, parentNodeDir));
                    archiveFolder(node, nodePath.get());
                    Path childrenDir = nodePath.get().resolve(node.getId()).resolve("children");
                    nodePath.set(childrenDir);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                nodePath.set(parentNodeDir);
            }
        });
        return nodePath.get();
    }

    private NodeInfo readNodeInfo(NodeInfo parentNodeInfo, Path nodeDir, UnarchiveContext context) throws IOException {
        NodeInfo nodeInfo;
        NodeInfo newNodeInfo;
        try (Reader reader = Files.newBufferedReader(nodeDir.resolve("info.json"), StandardCharsets.UTF_8)) {
            nodeInfo = mapper.readerFor(NodeInfo.class).readValue(reader);
            newNodeInfo = storage.createNode(context.getIdMapping(parentNodeInfo.getId()), nodeInfo.getName(), nodeInfo.getPseudoClass(), nodeInfo.getDescription(),
                        nodeInfo.getVersion(), nodeInfo.getGenericMetadata());
            context.getIdMapping().put(nodeInfo.getId(), newNodeInfo.getId());
        }

        LOGGER.info("Unarchiving node {} to {} ({})", nodeInfo.getId(), newNodeInfo.getId(), nodeInfo.getName());

        return newNodeInfo;
    }

    private List<ArchiveDependency> readDependencies(Path nodeDir) throws IOException {
        Path dependenciesFile = nodeDir.resolve("dependencies.json");
        if (Files.exists(dependenciesFile)) {
            try (Reader reader = Files.newBufferedReader(dependenciesFile, StandardCharsets.UTF_8)) {
                return mapper.readerFor(new TypeReference<List<ArchiveDependency>>() {
                }).readValue(reader);
            }
        }
        return Collections.emptyList();
    }

    private void readData(NodeInfo newNodeInfo, Path nodeDir) throws IOException {
        Path dataDir = nodeDir.resolve("data");
        if (Files.exists(dataDir)) {
            Set<String> dataNames = new HashSet<>();
            try (Stream<Path> stream = Files.list(dataDir)) {
                stream.forEach(dataFile -> {
                    try {
                        String dataFileName = dataFile.getFileName().toString();
                        String dataName = URLDecoder.decode(dataFileName.substring(0, dataFileName.length() - 3), StandardCharsets.UTF_8.name());
                        dataNames.add(dataName);
                        try (InputStream is = new GZIPInputStream(Files.newInputStream(dataFile));
                             OutputStream os = storage.writeBinaryData(newNodeInfo.getId(), dataName)) {
                            ByteStreams.copy(is, os);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            LOGGER.info("   {} data read", dataNames.size());
        }
    }

    private void readTimeSeries(NodeInfo newNodeInfo, Path nodeDir) throws IOException {
        Path timeSeriesDir = nodeDir.resolve("time-series");
        if (Files.exists(timeSeriesDir)) {
            Set<String> timeSeriesNames = new HashSet<>();
            int[] chunkCount = new int[1];
            try (Stream<Path> stream = Files.list(timeSeriesDir)) {
                stream.forEach(timeSeriesNameDir -> {
                    try {
                        String timeSeriesName = URLDecoder.decode(timeSeriesNameDir.getFileName().toString(), StandardCharsets.UTF_8.name());
                        timeSeriesNames.add(timeSeriesName);
                        TimeSeriesMetadata metadata;
                        try (Reader reader = Files.newBufferedReader(timeSeriesNameDir.resolve("metadata.json"), StandardCharsets.UTF_8)) {
                            metadata = mapper.readerFor(TimeSeriesMetadata.class).readValue(reader);
                            storage.createTimeSeries(newNodeInfo.getId(), metadata);
                        }
                        try (DirectoryStream<Path> chunksStream = Files.newDirectoryStream(timeSeriesNameDir, "chunks-*.json.gz")) {
                            for (Path chunksFile : chunksStream) {
                                Matcher matcher = CHUNKS_PATTERN.matcher(chunksFile.getFileName().toString());
                                if (!matcher.matches()) {
                                    throw new AssertionError("Invalid chunks file pattern");
                                }
                                int version = Integer.parseInt(matcher.group(1));
                                try (Reader reader = new InputStreamReader(new GZIPInputStream(Files.newInputStream(chunksFile)), StandardCharsets.UTF_8)) {
                                    switch (metadata.getDataType()) {
                                        case DOUBLE:
                                            List<DoubleDataChunk> doubleChunks = mapper.readerFor(new TypeReference<List<DoubleDataChunk>>() {
                                            }).readValue(reader);
                                            chunkCount[0] += doubleChunks.size();
                                            storage.addDoubleTimeSeriesData(newNodeInfo.getId(), version, timeSeriesName, doubleChunks);
                                            break;

                                        case STRING:
                                            List<StringDataChunk> stringChunks = mapper.readerFor(new TypeReference<List<StringDataChunk>>() {
                                            }).readValue(reader);
                                            chunkCount[0] += stringChunks.size();
                                            storage.addStringTimeSeriesData(newNodeInfo.getId(), version, timeSeriesName, stringChunks);
                                            break;

                                        default:
                                            throw new AssertionError("Unsupported data type " + metadata.getDataType());
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            LOGGER.info("   {} time series read ({} chunks)", timeSeriesNames.size(), chunkCount[0]);
        }
    }

    public void unarchiveChildren(NodeInfo parentNodeInfo, Path nodeDir) {
        Objects.requireNonNull(parentNodeInfo);
        Objects.requireNonNull(nodeDir);
        try {
            UnarchiveContext context = new UnarchiveContext();
            unarchiveChildren(parentNodeInfo, nodeDir, context);
            resolveDependencies(context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void unarchiveChildren(NodeInfo parentNodeInfo, Path nodeDir, UnarchiveContext context) throws IOException {
        Objects.requireNonNull(parentNodeInfo);
        Objects.requireNonNull(nodeDir);
        Objects.requireNonNull(context);
        Path childrenDir = nodeDir.resolve("children");
        if (Files.exists(childrenDir)) {
            try (Stream<Path> stream = Files.list(childrenDir)) {
                stream.forEach(childNodeDir -> {
                    try {
                        unarchive(parentNodeInfo, childNodeDir, context);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }
    }

    public void unarchiveDependencies(NodeInfo parentNodeInfo, Path nodeDir, UnarchiveContext context) throws IOException {
        Objects.requireNonNull(parentNodeInfo);
        Objects.requireNonNull(nodeDir);
        Objects.requireNonNull(context);
        Path dependenciesDir = nodeDir.resolve("dependencies");
        if (Files.exists(dependenciesDir)) {
            try (Stream<Path> stream = Files.list(dependenciesDir)) {
                stream.forEach(dependenciesNodeDir -> {
                    try {
                        unarchive(parentNodeInfo, dependenciesNodeDir, context);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }
    }

    private void unarchive(NodeInfo parentNodeInfo, Path nodeDir, UnarchiveContext context) throws IOException {

        NodeInfo newNodeInfo = readNodeInfo(parentNodeInfo, nodeDir, context);

        context.getDependencies().put(newNodeInfo.getId(), readDependencies(nodeDir));

        unarchiveDependencies(parentNodeInfo, nodeDir, context);

        readData(newNodeInfo, nodeDir);

        readTimeSeries(newNodeInfo, nodeDir);

        storage.setConsistent(newNodeInfo.getId());
        storage.flush();

        unarchiveChildren(newNodeInfo, nodeDir, context);
    }

    public void unarchive(Path nodeDir) {
        unarchive(null, nodeDir);
    }

    private void resolveDependencies(UnarchiveContext context) {
        for (Map.Entry<String, List<ArchiveDependency>> e : context.getDependencies().entrySet()) {
            String newNodeId = e.getKey();
            for (ArchiveDependency dependency : e.getValue()) {
                storage.addDependency(newNodeId, dependency.getName(), context.getIdMapping(dependency.getNodeId()));
            }
        }
    }

    public void unarchive(NodeInfo parentNodeInfo, Path nodeDir) {
        UnarchiveContext context = new UnarchiveContext();

        try {
            unarchive(parentNodeInfo, nodeDir, context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        resolveDependencies(context);
    }
}
