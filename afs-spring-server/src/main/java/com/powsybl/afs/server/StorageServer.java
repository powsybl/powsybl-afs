/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server;

import com.google.common.base.Strings;
import com.powsybl.afs.*;
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.buffer.*;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptionsBuilder;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import io.swagger.v3.oas.annotations.*;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping(value = "/rest/afs/" + StorageServer.API_VERSION)
@ComponentScan(basePackageClasses = {AppDataWrapper.class, StorageServer.class})
@Tag(name = "afs")
public class StorageServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageServer.class);
    public static final String API_VERSION = "v1";

    private final AppDataWrapper appDataWrapper;

    @Autowired
    public StorageServer(AppDataWrapper appDataWrapper) {
        this.appDataWrapper = appDataWrapper;
    }

    @GetMapping(value = "fileSystems")
    @Operation(summary = "Get file system list", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = List.class))),
        @ApiResponse(responseCode = "200", description = "The list of available file systems"),
        @ApiResponse(responseCode = "404", description = "There is no file system available.")})
    public List<String> getFileSystemNames() {
        return appDataWrapper.getAppData().getRemotelyAccessibleFileSystemNames();
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/rootNode", produces = "application/json")
    @Operation(summary = "Get file system root node and create it if not exist", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = NodeInfo.class))),
        @ApiResponse(responseCode = "200", description = "The root node"),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<NodeInfo> createRootNodeIfNotExists(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                              @Parameter(description = "Root node name") @RequestParam("nodeName") String nodeName,
                                                              @Parameter(description = "Root node pseudo class") @RequestParam("nodePseudoClass") String nodePseudoClass) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists(nodeName, nodePseudoClass);
        return ok(rootNodeInfo);
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/check/types", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get supported file system checks", responses = {
        @ApiResponse(responseCode = "200", description = "Ok", content = @Content(schema = @Schema(implementation = List.class))),
        @ApiResponse(responseCode = "404", description = "File system not found"),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<List<String>> check(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        return ok(storage.getSupportedFileSystemChecks());
    }

    @PostMapping("fileSystems/{fileSystemName}/check")
    @Operation(summary = "Check file system", responses = {
        @ApiResponse(responseCode = "200", description = "Check results", content = @Content(schema = @Schema(implementation = FileSystemCheckIssue.class))),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<List<FileSystemCheckIssue>> check(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                            @Parameter(description = "Issue types to check") @RequestParam("types") String types,
                                                            @Parameter(description = "Inconsistent if older than") @RequestParam(value = "instant", required = false) Long instant,
                                                            @Parameter(description = "Try to repair or not") @RequestParam(value = "repair", required = false) boolean repair) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        FileSystemCheckOptionsBuilder builder = new FileSystemCheckOptionsBuilder();
        if (!Strings.isNullOrEmpty(types)) {
            builder.addCheckTypes(Arrays.asList(types.split(",")));
        }
        if (instant != null) {
            Instant exp = Instant.ofEpochSecond(instant);
            builder.setInconsistentNodesExpirationTime(exp);
        }
        if (Boolean.TRUE.equals(repair)) {
            builder.repair();
        }
        return ok(storage.checkFileSystem(builder.build()));
    }

    @PostMapping(value = "fileSystems/{fileSystemName}/flush", consumes = "application/json")
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> flush(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                        @Parameter(description = "Storage Change Set") @RequestBody StorageChangeSet changeSet) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);

        for (StorageChange change : changeSet.getChanges()) {
            switch (change.getType()) {
                case TIME_SERIES_CREATION:
                    TimeSeriesCreation creation = (TimeSeriesCreation) change;
                    storage.createTimeSeries(creation.getNodeId(), creation.getMetadata());
                    break;
                case DOUBLE_TIME_SERIES_CHUNKS_ADDITION:
                    DoubleTimeSeriesChunksAddition doubleAddition = (DoubleTimeSeriesChunksAddition) change;
                    storage.addDoubleTimeSeriesData(doubleAddition.getNodeId(), doubleAddition.getVersion(),
                        doubleAddition.getTimeSeriesName(), doubleAddition.getChunks());
                    break;
                case STRING_TIME_SERIES_CHUNKS_ADDITION:
                    StringTimeSeriesChunksAddition stringAddition = (StringTimeSeriesChunksAddition) change;
                    storage.addStringTimeSeriesData(stringAddition.getNodeId(), stringAddition.getVersion(),
                        stringAddition.getTimeSeriesName(), stringAddition.getChunks());
                    break;
                default:
                    throw new AssertionError("Unknown change type " + change.getType());
            }
        }
        // propagate flush to underlying storage
        storage.flush();
        return ok();
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/writable", produces = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Boolean.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> isWritable(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                             @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        boolean writable = storage.isWritable(nodeId);
        return ok(Boolean.toString(writable));
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/consistent", produces = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Boolean.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> isConsistent(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                               @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        boolean consistent = storage.isConsistent(nodeId);
        return ok(Boolean.toString(consistent));
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/parent", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get Parent Node", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = NodeInfo.class))),
        @ApiResponse(responseCode = "200", description = "Returns the parent node"),
        @ApiResponse(responseCode = "404", description = "No parent node for nodeId"),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<NodeInfo> getParentNode(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                  @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        return okIfPresent(storage.getParentNode(nodeId));
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = InputStream.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<NodeInfo> getNodeInfo(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        NodeInfo nodeInfo = storage.getNodeInfo(nodeId);
        return ok(nodeInfo);
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/children", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get child nodes", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = List.class))),
        @ApiResponse(responseCode = "200", description = "The list of chid nodes"),
        @ApiResponse(responseCode = "404", description = "Thera are no child nodes"),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<List<NodeInfo>> getChildNodes(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                        @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        List<NodeInfo> childNodes = storage.getChildNodes(nodeId);
        return ok(childNodes);
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/inconsistentChildNodes", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get inconsistent child nodes", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = List.class))),
        @ApiResponse(responseCode = "200", description = "The list of inconsistent chid nodes"),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<List<NodeInfo>> getInconsistentNodes(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        List<NodeInfo> childNodes = storage.getInconsistentNodes();
        return ok(childNodes);
    }

    @PostMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Create Node", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = NodeInfo.class))),
        @ApiResponse(responseCode = "200", description = "The node is created"),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<NodeInfo> createNode(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                               @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                               @Parameter(description = "Child Name") @PathVariable("childName") String childName,
                                               @Parameter(description = "Description") @RequestParam("description") String description,
                                               @Parameter(description = "Node Pseudo Class") @RequestParam("nodePseudoClass") String nodePseudoClass,
                                               @Parameter(description = "Version") @RequestParam("version") int version,
                                               @Parameter(description = "Node Meta Data") @RequestBody NodeGenericMetadata nodeMetadata) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        try {
            NodeInfo newNodeInfo = storage.createNode(nodeId, childName, nodePseudoClass, description, version, nodeMetadata);
            return ok(newNodeInfo);
        } catch (AfsStorageException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
        }
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/name")
    @Operation(summary = "Rename Node", responses = {
        @ApiResponse(responseCode = "200", description = "The node is renamed"),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> renameNode(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                             @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                             @Parameter(description = "New node's name") @RequestBody String name) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.renameNode(nodeId, name);
        return ok();
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get Child Node", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = NodeInfo.class))),
        @ApiResponse(responseCode = "200", description = "Returns the child node"),
        @ApiResponse(responseCode = "404", description = "No child node for nodeId"),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<NodeInfo> getChildNode(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                 @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                 @Parameter(description = "Child Name") @PathVariable("childName") String childName) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        return storage.getChildNode(nodeId, childName)
            .map(StorageServer::ok)
            .orElseGet(StorageServer::noContent);
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/description", consumes = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> setDescription(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                 @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                 @Parameter(description = "Description") @RequestBody String description) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.setDescription(nodeId, description);
        return ok();
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/consistent", consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> consistent(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                             @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.setConsistent(nodeId);
        return ok();
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/modificationTime")
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> updateModificationTime(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                         @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.updateModificationTime(nodeId);
        return ok();
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Set.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<Set<NodeDependency>> getDependencies(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                               @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        Set<NodeDependency> dependencies = storage.getDependencies(nodeId);
        return ok(dependencies);
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/backwardDependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Set.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<Set<NodeInfo>> getBackwardDependencies(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                                 @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        Set<NodeInfo> backwardDependencyNodes = storage.getBackwardDependencies(nodeId);
        return ok(backwardDependencyNodes);
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> writeBinaryData(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                  @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                  @Parameter(description = "Name") @PathVariable("name") String name,
                                                  @Parameter(description = "Binary Data") InputStream is) throws IOException {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        try (OutputStream os = storage.writeBinaryData(nodeId, name)) {
            if (os != null) {
                IOUtils.copy(is, os);
            }
            return ok();
        }
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Set.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<Set<String>> getDataNames(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                    @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        Set<String> dataNames = storage.getDataNames(nodeId);
        return ok(dataNames);
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Add dependency to Node", responses = {
        @ApiResponse(responseCode = "200", description = "Dependency is added"),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> addDependency(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                @Parameter(description = "Name") @PathVariable("name") String name,
                                                @Parameter(description = "To Node ID") @PathVariable("toNodeId") String toNodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.addDependency(nodeId, name, toNodeId);
        return ok();
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Set.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<Set<NodeInfo>> getDependencies(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                         @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                         @Parameter(description = "Name") @PathVariable("name") String name) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        Set<NodeInfo> dependencies = storage.getDependencies(nodeId, name);
        return ok(dependencies);
    }

    @DeleteMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}")
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> removeDependency(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                   @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                   @Parameter(description = "Name") @PathVariable("name") String name,
                                                   @Parameter(description = "To Node ID") @PathVariable("toNodeId") String toNodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.removeDependency(nodeId, name, toNodeId);
        return ok();
    }

    @DeleteMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}", produces = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> deleteNode(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                             @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        String parentNodeId = storage.deleteNode(nodeId);
        return ok(parentNodeId);
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = InputStream.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<StreamingResponseBody> readBinaryAttribute(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                                     @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                                     @Parameter(description = "Name") @PathVariable("name") String name) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        return okIfPresent(storage.readBinaryData(nodeId, name).map(StorageServer::copyToBodyAndClose));
    }

    private static StreamingResponseBody copyToBodyAndClose(InputStream inputStream) {
        return outputStream -> {
            try (InputStream toClose = inputStream) {
                IOUtils.copy(toClose, outputStream);
            }
        };
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}", produces = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = String.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> dataExists(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                             @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                             @Parameter(description = "Name") @PathVariable("name") String name) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        boolean exists = storage.dataExists(nodeId, name);
        return ok(Boolean.toString(exists));
    }

    @DeleteMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}", produces = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Boolean.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> removeData(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                             @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                             @Parameter(description = "Data name") @PathVariable("name") String name) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        boolean removed = storage.removeData(nodeId, name);
        return ok(Boolean.toString(removed));
    }

    @PostMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries", consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> createTimeSeries(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                   @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                   @Parameter(description = "Time Series Meta Data") TimeSeriesMetadata metadata) {

        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.createTimeSeries(nodeId, metadata);
        return ok();
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/name", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Set.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<Set<String>> getTimeSeriesNames(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                          @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        Set<String> timeSeriesNames = storage.getTimeSeriesNames(nodeId);
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .body(timeSeriesNames);

    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}", produces = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Boolean.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> timeSeriesExists(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                   @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                   @Parameter(description = "Time series name") @PathVariable("timeSeriesName") String timeSeriesName) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        boolean exists = storage.timeSeriesExists(nodeId, timeSeriesName);
        return ok(Boolean.toString(exists));
    }

    @PostMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/metadata", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = List.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<List<TimeSeriesMetadata>> getTimeSeriesMetadata(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                                          @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                                          @Parameter(description = "Time series names") @RequestBody Set<String> timeSeriesNames) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(nodeId, timeSeriesNames);
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .body(metadataList);
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/versions", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Set<Integer>> getTimeSeriesDataVersions(@PathVariable("fileSystemName") String fileSystemName,
                                                                  @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        Set<Integer> versions = storage.getTimeSeriesDataVersions(nodeId);
        return ok(versions);
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}/versions", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = Set.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<Set<Integer>> getTimeSeriesDataVersions(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                                  @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                                  @Parameter(description = "Time series name") @PathVariable("timeSeriesName") String timeSeriesName) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        Set<Integer> versions = storage.getTimeSeriesDataVersions(nodeId, timeSeriesName);
        return ok(versions);
    }

    @PostMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = List.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<Map<String, List<DoubleDataChunk>>> getDoubleTimeSeriesData(@PathVariable("fileSystemName") String fileSystemName,
                                                                                      @PathVariable("nodeId") String nodeId,
                                                                                      @PathVariable("version") int version,
                                                                                      @RequestBody Set<String> timeSeriesNames) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        Map<String, List<DoubleDataChunk>> timeSeriesData = storage.getDoubleTimeSeriesData(nodeId, timeSeriesNames, version);
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .body(timeSeriesData);
    }

    @PostMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/string/{version}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(content = @Content(schema = @Schema(implementation = List.class))),
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<Map<String, List<StringDataChunk>>> getStringTimeSeriesData(@PathVariable("fileSystemName") String fileSystemName,
                                                                                      @PathVariable("nodeId") String nodeId,
                                                                                      @PathVariable("version") int version,
                                                                                      @RequestBody Set<String> timeSeriesNames) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        Map<String, List<StringDataChunk>> timeSeriesData = storage.getStringTimeSeriesData(nodeId, timeSeriesNames, version);
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .body(timeSeriesData);
    }

    @DeleteMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries")
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> clearTimeSeries(@PathVariable("fileSystemName") String fileSystemName,
                                                  @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.clearTimeSeries(nodeId);
        return ok();
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/parent", consumes = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> setParentNode(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                @Parameter(description = "New Parent Node ID") @RequestBody String newParentNodeId) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.setParentNode(nodeId, newParentNodeId);
        return ok();
    }

    @GetMapping(value = "fileSystems/{fileSystemName}/tasks", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TaskMonitor.Snapshot> takeSnapshot(@PathVariable("fileSystemName") String fileSystemName,
                                                             @RequestParam("projectId") String projectId) {
        AppFileSystem fileSystem = appDataWrapper.getFileSystem(fileSystemName);
        TaskMonitor.Snapshot snapshot = fileSystem.getTaskMonitor().takeSnapshot(projectId);
        return ok(snapshot);
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/tasks", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Start a task", responses = {
        @ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = TaskMonitor.Task.class))),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<TaskMonitor.Task> startTask(@PathVariable("fileSystemName") String fileSystemName,
                                                      @RequestParam(value = "projectFileId", required = false) String projectFileId,
                                                      @RequestParam(value = "projectId", required = false) String projectId,
                                                      @RequestParam(value = "name", required = false) String name) {
        logInfo("Starting task {} for node {} ({})", name, projectFileId, projectId);
        AppFileSystem fileSystem = appDataWrapper.getFileSystem(fileSystemName);
        TaskMonitor.Task task;
        if (projectFileId != null) {
            ProjectFile projectFile = fileSystem.findProjectFile(projectFileId, ProjectFile.class);
            if (projectFile == null) {
                throw new AfsException("Project file '" + projectFileId + "' not found in file system '" + fileSystemName + "'");
            }
            task = fileSystem.getTaskMonitor().startTask(projectFile);
        } else if (projectId != null && name != null) {
            Project project = fileSystem
                .findProject(projectId)
                .orElseThrow(() -> new AfsException("Project '" + projectId + "' not found in file system '" + fileSystemName + "'"));
            task = fileSystem.getTaskMonitor().startTask(name, project);
        } else {
            throw new AfsException("Missing arguments");
        }
        return ResponseEntity.ok(task);
    }

    @PostMapping(value = "fileSystems/{fileSystemName}/tasks/{taskId}", consumes = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> updateTaskMessage(@PathVariable("fileSystemName") String fileSystemName,
                                                    @PathVariable("taskId") UUID taskId,
                                                    @RequestBody String message) {
        AppFileSystem fileSystem = appDataWrapper.getFileSystem(fileSystemName);
        fileSystem.getTaskMonitor().updateTaskMessage(taskId, message);
        return ok();
    }

    @DeleteMapping(value = "fileSystems/{fileSystemName}/tasks/{taskId}")
    public ResponseEntity<String> stopTask(@PathVariable("fileSystemName") String fileSystemName,
                                           @PathVariable("taskId") UUID taskId) {
        AppFileSystem fileSystem = appDataWrapper.getFileSystem(fileSystemName);
        fileSystem.getTaskMonitor().stopTask(taskId);
        return ok();
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/tasks/{taskId}/_cancel", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Cancel a task tracked process", responses = {
        @ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = Boolean.class))),
        @ApiResponse(responseCode = "404", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<Boolean> cancel(@PathVariable("fileSystemName") String fileSystemName,
                                          @PathVariable("taskId") String taskId) {
        logInfo("Canceling task {}", taskId);
        AppFileSystem fileSystem = appDataWrapper.getFileSystem(fileSystemName);
        boolean success = fileSystem.getTaskMonitor().cancelTaskComputation(UUID.fromString(taskId));
        return ResponseEntity.ok(success);
    }

    @PostMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}/{timeSeriesName}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> addDoubleTimeSeriesData(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                          @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                          @Parameter(description = "Version") @PathVariable("version") int version,
                                                          @Parameter(description = "Time series name") @PathVariable("timeSeriesName") String timeSeriesName,
                                                          @Parameter(description = "List double array chunk") @RequestBody List<DoubleDataChunk> chunks) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.addDoubleTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        return ok();
    }

    @PostMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/string/{version}/{timeSeriesName}", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @Operation(summary = "", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> addStringTimeSeriesData(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                                          @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                                          @Parameter(description = "Version") @PathVariable("version") int version,
                                                          @Parameter(description = "Time Series Name") @PathVariable("timeSeriesName") String timeSeriesName,
                                                          @Parameter(description = "List string array chunkFile system name") @RequestBody List<StringDataChunk> chunks) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.addStringTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        return ok();
    }

    @PutMapping(value = "fileSystems/{fileSystemName}/nodes/{nodeId}/metadata", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Update node's metadata", responses = {
        @ApiResponse(responseCode = "200", description = ""),
        @ApiResponse(responseCode = "500", description = "Error")})
    public ResponseEntity<String> setMetadata(@Parameter(description = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                              @Parameter(description = "Node ID") @PathVariable("nodeId") String nodeId,
                                              @Parameter(description = "Node Meta Data") @RequestBody NodeGenericMetadata nodeMetadata) {
        AppStorage storage = appDataWrapper.getStorage(fileSystemName);
        storage.setMetadata(nodeId, nodeMetadata);
        return ok();
    }

    private static <T> ResponseEntity<T> ok() {
        return ResponseEntity.ok().build();
    }

    private static <T> ResponseEntity<T> noContent() {
        return ResponseEntity.noContent().build();
    }

    private static <T> ResponseEntity<T> ok(T body) {
        return ResponseEntity.ok(body);
    }

    private static <T> ResponseEntity<T> okIfPresent(Optional<T> body) {
        return body
            .map(StorageServer::ok)
            .orElseGet(StorageServer::noContent);
    }

    private static void logInfo(String message, Object... params) {
        if (LOGGER.isInfoEnabled()) {
            Object[] objects = Arrays.stream(params)
                .map(StorageServer::encode)
                .toArray();
            LOGGER.info(message, objects);
        }
    }

    private static Object encode(Object input) {
        if (input instanceof String) {
            return ((String) input).replaceAll("[\n\r\t]", "_");
        }
        return input;
    }
}
