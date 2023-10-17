/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.powsybl.afs.*;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeDependency;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.buffer.*;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptionsBuilder;
import com.powsybl.afs.ws.server.utils.AppDataBean;
import com.powsybl.afs.ws.server.utils.JwtTokenNeeded;
import com.powsybl.afs.ws.utils.AfsRestApi;
import com.powsybl.afs.ws.utils.gzip.Compress;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import io.swagger.v3.oas.annotations.*;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.*;

/**
 * @author Ali Tahanout <ali.tahanout at rte-france.com>
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Named
@ApplicationScoped
@Path(AfsRestApi.RESOURCE_ROOT + "/" + AfsRestApi.VERSION)
@JwtTokenNeeded
public class AppStorageServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppStorageServer.class);

    @Inject
    private AppDataBean appDataBean;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems")
    @Operation(summary = "Get file system list", responses = {@ApiResponse(responseCode = "200", description = "The list of available file systems", content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))), @ApiResponse(responseCode = "404", description = "There is no file system available.")})
    public Response getFileSystemNames() {
        return Response.ok().entity(appDataBean.getAppData().getRemotelyAccessibleFileSystemNames()).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/rootNode")
    @Operation (summary = "Get file system root node and create it if not exist", responses = {@ApiResponse(responseCode = "200", description = "The root node", content = @Content(schema = @Schema(implementation = NodeInfo.class))), @ApiResponse(responseCode = "500", description = "Error")})
    public Response createRootNodeIfNotExists(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                              @Parameter(description = "Root node name") @QueryParam("nodeName") String nodeName,
                                              @Parameter(description = "Root node pseudo class") @QueryParam("nodePseudoClass") String nodePseudoClass) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists(nodeName, nodePseudoClass);
        return Response.ok().entity(rootNodeInfo).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/check")
    @Operation (summary = "Check file system", responses = {@ApiResponse(responseCode = "200", description = "Check results", content = @Content(schema = @Schema(implementation = FileSystemCheckIssue.class))), @ApiResponse(responseCode = "500", description = "Error")})
    public Response check(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                          @Parameter(description = "Issue types to check") @QueryParam("types") String types,
                          @Parameter(description = "Inconsistent if older than") @QueryParam("instant") Long instant,
                          @Parameter(description = "Try to repair or not") @QueryParam("repair") boolean repair) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        FileSystemCheckOptionsBuilder builder = new FileSystemCheckOptionsBuilder();
        if (!Strings.isNullOrEmpty(types)) {
            builder.addCheckTypes(Arrays.asList(types.split(",")));
        }
        if (instant != null) {
            Instant exp = Instant.ofEpochSecond(instant);
            builder.setInconsistentNodesExpirationTime(exp);
        }
        if (repair) {
            builder.repair();
        }
        return Response.ok().entity(storage.checkFileSystem(builder.build())).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/children")
    @Operation (summary = "Get child nodes", responses = {@ApiResponse(responseCode = "200", description = "The list of chid nodes", content = @Content(array = @ArraySchema(schema = @Schema(implementation = NodeInfo.class)))), @ApiResponse(responseCode = "404", description = "Thera are no child nodes"), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getChildNodes(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                  @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        List<NodeInfo> childNodes = storage.getChildNodes(nodeId);
        return Response.ok().entity(childNodes).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/inconsistentChildNodes")
    @Operation (summary = "Get inconsistent child nodes", responses = {@ApiResponse(responseCode = "200", description = "The list of inconsistent chid nodes", content = @Content(array = @ArraySchema(schema = @Schema(implementation = NodeInfo.class)))), @ApiResponse(responseCode = "404", description = "Thera are no inconsistent child nodes"), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getInconsistentChildrenNodes(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        List<NodeInfo> childNodes = storage.getInconsistentNodes();
        return Response.ok().entity(childNodes).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}")
    @Operation (summary = "Create Node", responses = {@ApiResponse(responseCode = "200", description = "The node is created", content = @Content(schema = @Schema(implementation = NodeInfo.class))), @ApiResponse(responseCode = "500", description = "Error")})
    public Response createNode(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                               @Parameter(description = "Child Name") @PathParam("childName") String childName,
                               @Parameter(description = "Description") @QueryParam("description") String description,
                               @Parameter(description = "Node Pseudo Class") @QueryParam("nodePseudoClass") String nodePseudoClass,
                               @Parameter(description = "Version") @QueryParam("version") int version,
                               @Parameter(description = "Node Meta Data") NodeGenericMetadata nodeMetadata) {
        logInfo("Creating node {} under parent {} with name {}", nodePseudoClass, childName, nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        NodeInfo newNodeInfo = storage.createNode(nodeId, childName, nodePseudoClass, description, version, nodeMetadata);
        return Response.ok().entity(newNodeInfo).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/metadata")
    @Operation (summary = "Update node's metadata", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response setMetadata(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                              @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                              @Parameter(description = "Node Meta Data") NodeGenericMetadata nodeMetadata) {
        logInfo("Updating metadata for node {}", nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setMetadata(nodeId, nodeMetadata);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/parent")
    @Operation (summary = "Get Parent Node", responses = {@ApiResponse(responseCode = "200", description = "Returns the parent node", content = @Content(schema = @Schema(implementation = NodeInfo.class))), @ApiResponse(responseCode = "404", description = "No parent node for nodeId"), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getParentNode(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                  @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Optional<NodeInfo> parentNodeInfo = storage.getParentNode(nodeId);
        if (parentNodeInfo.isPresent()) {
            return Response.ok().entity(parentNodeInfo.get()).build();
        } else {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}")
    @Operation (summary = "Get Child Node", responses = {@ApiResponse(responseCode = "200", description = "Returns the child node", content = @Content(schema = @Schema(implementation = NodeInfo.class))), @ApiResponse(responseCode = "404", description = "No child node for nodeId"), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getChildNode(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                 @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                 @Parameter(description = "Child Name") @PathParam("childName") String childName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Optional<NodeInfo> childNodeInfo = storage.getChildNode(nodeId, childName);
        if (childNodeInfo.isPresent()) {
            return Response.status(Status.OK).entity(childNodeInfo.get()).build();
        } else {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}")
    @Operation (summary = "Add dependency to Node", responses = {@ApiResponse(responseCode = "200", description = "Dependency is added"), @ApiResponse(responseCode = "500", description = "Error")})
    public Response addDependency(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                  @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                  @Parameter(description = "Name") @PathParam("name") String name,
                                  @Parameter(description = "To Node ID") @PathParam("toNodeId") String toNodeId) {
        logInfo("Adding dependency {} from {} to {}", name, nodeId, toNodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.addDependency(nodeId, name, toNodeId);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}")
    @Operation (summary = "Get node dependencies", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = NodeInfo.class)))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getDependencies(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                    @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                    @Parameter(description = "Name") @PathParam("name") String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<NodeInfo> dependencies = storage.getDependencies(nodeId, name);
        return Response.ok().entity(dependencies).build();
    }

    @DELETE
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}")
    @Operation (summary = "Delete node", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response deleteNode(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        logInfo("Deleting node {}", nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        String parentNodeId = storage.deleteNode(nodeId);
        return Response.ok(parentNodeId).build();
    }

    @PUT
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/description")
    @Operation (summary = "Update node description", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response setDescription(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                   @Parameter(description = "File system name") @PathParam("nodeId") String nodeId,
                                   @Parameter(description = "Description") String description) {
        logInfo("Updating description for node {}", nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setDescription(nodeId, description);
        return Response.ok().build();
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/consistent")
    @Operation (summary = "Validate node's consistency", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response setConsistent(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                   @Parameter(description = "File system name") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setConsistent(nodeId);
        return Response.ok().build();
    }

    @PUT
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/name")
    @Operation (summary = "Rename node", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response renameNode(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                   @Parameter(description = "File system name") @PathParam("nodeId") String nodeId,
                                   @Parameter(description = "Name") String name) {
        logInfo("Renaming node {} to {}", nodeId, name);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.renameNode(nodeId, name);
        return Response.ok().build();
    }

    @PUT
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/modificationTime")
    @Operation (summary = "Update node modification time", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response updateModificationTime(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                           @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.updateModificationTime(nodeId);
        return Response.ok().build();
    }

    @PUT
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}")
    @Operation (summary = "Update node data", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response writeBinaryData(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                    @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                    @Parameter(description = "Name") @PathParam("name") String name,
                                    @Parameter(description = "Binary Data") InputStream is) {
        logInfo("Updating data {} for node {}", name, nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        try (OutputStream os = storage.writeBinaryData(nodeId, name)) {
            if (os == null) {
                return Response.noContent().build();
            } else {
                ByteStreams.copy(is, os);
                return Response.ok().build();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}")
    @Operation (summary = "Fetch node data", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = InputStream.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response readBinaryAttribute(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                        @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                        @Parameter(description = "Name") @PathParam("name") String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Optional<InputStream> is = storage.readBinaryData(nodeId, name);
        if (is.isPresent()) {
            return Response.ok().entity(is.get()).build();
        }
        return Response.noContent().build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}")
    @Operation (summary = "Indicate if given node provides given named data", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = Boolean.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response dataExists(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                               @Parameter(description = "Name") @PathParam("name") String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean exists = storage.dataExists(nodeId, name);
        return Response.ok().entity(exists).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/data")
    @Operation (summary = "Fetch all data names for given node", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getDataNames(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                 @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<String> dataNames = storage.getDataNames(nodeId);
        return Response.ok().entity(dataNames).build();
    }

    @DELETE
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}")
    @Operation (summary = "Remove named data for given node", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = Boolean.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response removeData(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                               @Parameter(description = "Data name") @PathParam("name") String name) {
        logInfo("Removing data {} for node {}", name, nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean removed = storage.removeData(nodeId, name);
        return Response.ok().entity(removed).build();
    }

    @PUT
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/parent")
    @Operation (summary = "Update parent for given node", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response setParentNode(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                  @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                  @Parameter(description = "New Parent Node ID") String newParentNodeId) {
        logInfo("Moving node {} under node {}", nodeId, newParentNodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setParentNode(nodeId, newParentNodeId);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/writable")
    @Operation (summary = "Indicate if given node is writable", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = Boolean.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response isWritable(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean writable = storage.isWritable(nodeId);
        return Response.ok().entity(writable).build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/consistent")
    @Operation (summary = "Indicate if given node is consistent", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = Boolean.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response isConsistent(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                 @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean isConsistent = storage.isConsistent(nodeId);
        return Response.ok().entity(isConsistent).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies")
    @Operation (summary = "Fetch node dependencies information", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(array = @ArraySchema(schema = @Schema(implementation = NodeDependency.class)))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getDependencies(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                    @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<NodeDependency> dependencies = storage.getDependencies(nodeId);
        return Response.ok().entity(dependencies).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/backwardDependencies")
    @Operation (summary = "Fetch node backward dependencies", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(array = @ArraySchema(schema = @Schema(implementation = NodeInfo.class)))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getBackwardDependencies(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<NodeInfo> backwardDependencyNodes = storage.getBackwardDependencies(nodeId);
        return Response.ok().entity(backwardDependencyNodes).build();
    }

    @DELETE
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}")
    @Operation (summary = "Remove dependency to node", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response removeDependency(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                     @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                     @Parameter(description = "Name") @PathParam("name") String name,
                                     @Parameter(description = "To Node ID") @PathParam("toNodeId") String toNodeId) {
        logInfo("Removing dependency {} ({} -> {})", name, nodeId, toNodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.removeDependency(nodeId, name, toNodeId);
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/flush")
    @Operation (summary = "Flush appstorage changeset", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response flush(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                          @Parameter(description = "Storage Change Set") StorageChangeSet changeSet) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);

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

        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries")
    @Operation (summary = "Create new timeseries for given node", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response createTimeSeries(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                     @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                     @Parameter(description = "Time Series Meta Data") TimeSeriesMetadata metadata) {
        logInfo("Creating timeseries {} for node {}", metadata.getName(), nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.createTimeSeries(nodeId, metadata);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/name")
    @Compress
    @Operation (summary = "Fetch node timeseries", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getTimeSeriesNames(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                       @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<String> timeSeriesNames = storage.getTimeSeriesNames(nodeId);
        return Response.ok().header(HttpHeaders.CONTENT_ENCODING, "gzip").entity(timeSeriesNames).build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}")
    @Operation (summary = "Indicate if named timeseries exist for given node", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = Boolean.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response timeSeriesExists(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                     @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                     @Parameter(description = "Time series name") @PathParam("timeSeriesName") String timeSeriesName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean exists = storage.timeSeriesExists(nodeId, timeSeriesName);
        return Response.ok().entity(exists).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/metadata")
    @Compress
    @Operation (summary = "Fetch timeseries metadata for given names and node", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(array = @ArraySchema(schema = @Schema(implementation = TimeSeriesMetadata.class)))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getTimeSeriesMetadata(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                          @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                          @Parameter(description = "Time series names") Set<String> timeSeriesNames) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(nodeId, timeSeriesNames);
        return Response.ok().header(HttpHeaders.CONTENT_ENCODING, "gzip").entity(metadataList).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/versions")
    @Operation (summary = "Fetch timeseries available versions", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(array = @ArraySchema(schema = @Schema(implementation = Integer.class)))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getTimeSeriesDataVersions(@PathParam("fileSystemName") String fileSystemName,
                                              @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<Integer> versions = storage.getTimeSeriesDataVersions(nodeId);
        return Response.ok().entity(versions).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}/versions")
    @Operation (summary = "Fetch timeseries available versions for given named timeseries", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(array = @ArraySchema(schema = @Schema(implementation = Integer.class)))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getTimeSeriesDataVersions(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                              @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                              @Parameter(description = "Time series name") @PathParam("timeSeriesName") String timeSeriesName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<Integer> versions = storage.getTimeSeriesDataVersions(nodeId, timeSeriesName);
        return Response.ok().entity(versions).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}/{timeSeriesName}")
    @Operation (summary = "Append Double data chunks to name timeseries", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response addDoubleTimeSeriesData(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                            @Parameter(description = "Version") @PathParam("version") int version,
                                            @Parameter(description = "Time series name") @PathParam("timeSeriesName") String timeSeriesName,
                                            @Parameter(description = "List double array chunk") List<DoubleDataChunk> chunks) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.addDoubleTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}")
    @Compress
    @Operation (summary = "Fetch Double data for named timeseries", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = DoubleDataChunk.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getDoubleTimeSeriesData(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                            @Parameter(description = "Version") @PathParam("version") int version,
                                            @Parameter(description = "Set time series names") Set<String> timeSeriesNames) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Map<String, List<DoubleDataChunk>> timeSeriesData = storage.getDoubleTimeSeriesData(nodeId, timeSeriesNames, version);
        return Response.ok()
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
                .entity(timeSeriesData)
                .build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/string/{version}/{timeSeriesName}")
    @Operation (summary = "Append String data chunks to named timeseries", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response addStringTimeSeriesData(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @Parameter(description = "File system name") @PathParam("nodeId") String nodeId,
                                            @Parameter(description = "File system name") @PathParam("version") int version,
                                            @Parameter(description = "File system name") @PathParam("timeSeriesName") String timeSeriesName,
                                            @Parameter(description = "List string array chunkFile system name") List<StringDataChunk> chunks) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.addStringTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/string/{version}")
    @Compress
    @Operation (summary = "Fetch String data for named timeseries", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = StringDataChunk.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getStringTimeSeriesData(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId,
                                            @Parameter(description = "Version") @PathParam("version") int version,
                                            @Parameter(description = "Set time series names") Set<String> timeSeriesNames) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Map<String, List<StringDataChunk>> timeSeriesData = storage.getStringTimeSeriesData(nodeId, timeSeriesNames, version);
        return Response.ok()
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
                .entity(timeSeriesData)
                .build();
    }

    @DELETE
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries")
    @Operation (summary = "Delete all timeseries for given node", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response clearTimeSeries(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                    @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        logInfo("Clearing timeseries for node {}", nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.clearTimeSeries(nodeId);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}")
    @Operation (summary = "Fetch node info", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = NodeInfo.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response getNodeInfo(@Parameter(description = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                @Parameter(description = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        NodeInfo nodeInfo = storage.getNodeInfo(nodeId);
        return Response.ok().entity(nodeInfo).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/tasks")
    @Operation (summary = "Start a task", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = TaskMonitor.Task.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response startTask(@PathParam("fileSystemName") String fileSystemName,
                              @QueryParam("projectFileId") String projectFileId,
                              @QueryParam("projectId") String projectId,
                              @QueryParam("name") String name) {
        logInfo("Starting task {} for node {} ({})", name, projectFileId, projectId);
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
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
        return Response.ok().entity(task).build();
    }

    @DELETE
    @Path("fileSystems/{fileSystemName}/tasks/{taskId}")
    @Operation (summary = "Stop task tracking", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response stopTask(@PathParam("fileSystemName") String fileSystemName,
                             @PathParam("taskId") UUID taskId) {
        logInfo("Stopping task {}", taskId);
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        fileSystem.getTaskMonitor().stopTask(taskId);
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/tasks/{taskId}")
    @Operation (summary = "Update a task message", responses = {@ApiResponse(responseCode = "200", description = ""), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response updateTaskMessage(@PathParam("fileSystemName") String fileSystemName,
                                      @PathParam("taskId") UUID taskId,
                                      String message) {
        logInfo("Updating task {} with message {}", taskId, message);
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        fileSystem.getTaskMonitor().updateTaskMessage(taskId, message);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/tasks")
    @Operation (summary = "Fetch current running tasks snapshot", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = TaskMonitor.Snapshot.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response takeSnapshot(@PathParam("fileSystemName") String fileSystemName,
                                 @QueryParam("projectId") String projectId) {
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        TaskMonitor.Snapshot snapshot = fileSystem.getTaskMonitor().takeSnapshot(projectId);
        return Response.ok().entity(snapshot).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/tasks/{taskId}/_cancel")
    @Operation (summary = "Cancel a task tracked process", responses = {@ApiResponse(responseCode = "200", description = "", content = @Content(schema = @Schema(implementation = Boolean.class))), @ApiResponse(responseCode = "404", description = ""), @ApiResponse(responseCode = "500", description = "Error")})
    public Response cancel(@PathParam("fileSystemName") String fileSystemName,
                                 @PathParam("taskId") String taskId) {
        logInfo("Canceling task {}", taskId);
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        boolean success = fileSystem.getTaskMonitor().cancelTaskComputation(UUID.fromString(taskId));
        return Response.ok(success).build();
    }

    private static void logInfo(String message, Object... params) {
        if (LOGGER.isInfoEnabled()) {
            Object[] objects = Arrays.stream(params)
                    .map(AppStorageServer::encode)
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
