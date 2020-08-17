/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server;

import com.google.common.io.ByteStreams;
import com.powsybl.afs.*;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeDependency;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.buffer.*;
import com.powsybl.afs.ws.server.utils.AppDataBean;
import com.powsybl.afs.ws.server.utils.JwtTokenNeeded;
import com.powsybl.afs.ws.utils.AfsRestApi;
import com.powsybl.afs.ws.utils.gzip.Compress;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import io.swagger.annotations.*;
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
import java.util.*;

/**
 * @author Ali Tahanout <ali.tahanout at rte-france.com>
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Named
@ApplicationScoped
@Path(AfsRestApi.RESOURCE_ROOT + "/" + AfsRestApi.VERSION)
@Api(value = "/afs", tags = "afs")
@JwtTokenNeeded
public class AppStorageServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppStorageServer.class);

    @Inject
    private AppDataBean appDataBean;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems")
    @ApiOperation (value = "Get file system list")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The list of available file systems", responseContainer = "List", response = String.class), @ApiResponse(code = 404, message = "There is no file system available.")})
    public Response getFileSystemNames() {
        return Response.ok().entity(appDataBean.getAppData().getRemotelyAccessibleFileSystemNames()).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/rootNode")
    @ApiOperation (value = "Get file system root node and create it if not exist")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The root node", response = NodeInfo.class), @ApiResponse(code = 500, message = "Error")})
    public Response createRootNodeIfNotExists(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                              @ApiParam(value = "Root node name") @QueryParam("nodeName") String nodeName,
                                              @ApiParam(value = "Root node pseudo class") @QueryParam("nodePseudoClass") String nodePseudoClass) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists(nodeName, nodePseudoClass);
        return Response.ok().entity(rootNodeInfo).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/children")
    @ApiOperation (value = "Get child nodes")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The list of chid nodes", response = NodeInfo.class, responseContainer = "List"), @ApiResponse(code = 404, message = "Thera are no child nodes"), @ApiResponse(code = 500, message = "Error")})
    public Response getChildNodes(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                  @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        List<NodeInfo> childNodes = storage.getChildNodes(nodeId);
        return Response.ok().entity(childNodes).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/inconsistentChildNodes")
    @ApiOperation (value = "Get inconsistent child nodes")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The list of inconsistent chid nodes", response = NodeInfo.class, responseContainer = "List"), @ApiResponse(code = 404, message = "Thera are no inconsistent child nodes"), @ApiResponse(code = 500, message = "Error")})
    public Response getInconsistentChildrenNodes(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        List<NodeInfo> childNodes = storage.getInconsistentNodes();
        return Response.ok().entity(childNodes).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}")
    @ApiOperation (value = "Create Node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The node is created", response = NodeInfo.class), @ApiResponse(code = 500, message = "Error")})
    public Response createNode(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                               @ApiParam(value = "Child Name") @PathParam("childName") String childName,
                               @ApiParam(value = "Description") @QueryParam("description") String description,
                               @ApiParam(value = "Node Pseudo Class") @QueryParam("nodePseudoClass") String nodePseudoClass,
                               @ApiParam(value = "Version") @QueryParam("version") int version,
                               @ApiParam(value = "Node Meta Data") NodeGenericMetadata nodeMetadata) {
        logInfo("Creating node {} under parent {} with name {}", nodePseudoClass, childName, nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        NodeInfo newNodeInfo = storage.createNode(nodeId, childName, nodePseudoClass, description, version, nodeMetadata);
        return Response.ok().entity(newNodeInfo).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/metadata")
    @ApiOperation (value = "Update node's metadata")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response setMetadata(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                              @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                              @ApiParam(value = "Node Meta Data") NodeGenericMetadata nodeMetadata) {
        logInfo("Udpating metadata for node {}", nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setMetadata(nodeId, nodeMetadata);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/parent")
    @ApiOperation (value = "Get Parent Node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "Returns the parent node", response = NodeInfo.class), @ApiResponse(code = 404, message = "No parent node for nodeId"), @ApiResponse(code = 500, message = "Error")})
    public Response getParentNode(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                  @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
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
    @ApiOperation (value = "Get Child Node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "Returns the child node", response = NodeInfo.class), @ApiResponse(code = 404, message = "No child node for nodeId"), @ApiResponse(code = 500, message = "Error")})
    public Response getChildNode(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                 @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                 @ApiParam(value = "Child Name") @PathParam("childName") String childName) {
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
    @ApiOperation (value = "Add dependency to Node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "Dependency is added"), @ApiResponse(code = 500, message = "Error")})
    public Response addDependency(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                  @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                  @ApiParam(value = "Name") @PathParam("name") String name,
                                  @ApiParam(value = "To Node ID") @PathParam("toNodeId") String toNodeId) {
        logInfo("Adding dependency {} from {} to {}", name, nodeId, toNodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.addDependency(nodeId, name, toNodeId);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}")
    @ApiOperation (value = "Get node dependencies")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = NodeInfo.class, responseContainer = "Set"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getDependencies(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                    @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                    @ApiParam(value = "Name") @PathParam("name") String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<NodeInfo> dependencies = storage.getDependencies(nodeId, name);
        return Response.ok().entity(dependencies).build();
    }

    @DELETE
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}")
    @ApiOperation (value = "Delete node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response deleteNode(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        logInfo("Deleting node {}", nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        String parentNodeId = storage.deleteNode(nodeId);
        return Response.ok(parentNodeId).build();
    }

    @PUT
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/description")
    @ApiOperation (value = "Update node description")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response setDescription(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                   @ApiParam(value = "File system name") @PathParam("nodeId") String nodeId,
                                   @ApiParam(value = "Description") String description) {
        logInfo("Updating description for node {}", nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setDescription(nodeId, description);
        return Response.ok().build();
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/consistent")
    @ApiOperation (value = "Validate node's consistency")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response setConsistent(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                   @ApiParam(value = "File system name") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setConsistent(nodeId);
        return Response.ok().build();
    }

    @PUT
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/name")
    @ApiOperation (value = "Rename node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response renameNode(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                   @ApiParam(value = "File system name") @PathParam("nodeId") String nodeId,
                                   @ApiParam(value = "Name") String name) {
        logInfo("Renaming node {} to {}", nodeId, name);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.renameNode(nodeId, name);
        return Response.ok().build();
    }

    @PUT
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/modificationTime")
    @ApiOperation (value = "Update node modification time")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response updateModificationTime(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                           @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.updateModificationTime(nodeId);
        return Response.ok().build();
    }

    @PUT
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}")
    @ApiOperation (value = "Update node data")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response writeBinaryData(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                    @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                    @ApiParam(value = "Name") @PathParam("name") String name,
                                    @ApiParam(value = "Binary Data") InputStream is) {
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
    @ApiOperation (value = "Fetch node data")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = InputStream.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response readBinaryAttribute(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                        @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                        @ApiParam(value = "Name") @PathParam("name") String name) {
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
    @ApiOperation (value = "Indicate if given node provides given named data")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = Boolean.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response dataExists(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                               @ApiParam(value = "Name") @PathParam("name") String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean exists = storage.dataExists(nodeId, name);
        return Response.ok().entity(exists).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/data")
    @ApiOperation (value = "Fetch all data names for given node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = String.class, responseContainer = "Set"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getDataNames(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                 @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<String> dataNames = storage.getDataNames(nodeId);
        return Response.ok().entity(dataNames).build();
    }

    @DELETE
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}")
    @ApiOperation (value = "Remove named data for given node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = Boolean.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response removeData(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                               @ApiParam(value = "Data name") @PathParam("name") String name) {
        logInfo("Removing data {} for node {}", name, nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean removed = storage.removeData(nodeId, name);
        return Response.ok().entity(removed).build();
    }

    @PUT
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/parent")
    @ApiOperation (value = "Update parent for given node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response setParentNode(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                  @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                  @ApiParam(value = "New Parent Node ID") String newParentNodeId) {
        logInfo("Moving node {} under node {}", nodeId, newParentNodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setParentNode(nodeId, newParentNodeId);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/writable")
    @ApiOperation (value = "Indicate if given node is writable")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = Boolean.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response isWritable(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                               @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean writable = storage.isWritable(nodeId);
        return Response.ok().entity(writable).build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/consistent")
    @ApiOperation (value = "Indicate if given node is consistent")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = Boolean.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response isConsistent(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                 @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean isConsistent = storage.isConsistent(nodeId);
        return Response.ok().entity(isConsistent).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies")
    @ApiOperation (value = "Fetch node dependencies information")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = NodeDependency.class, responseContainer = "List"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getDependencies(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                    @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<NodeDependency> dependencies = storage.getDependencies(nodeId);
        return Response.ok().entity(dependencies).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/backwardDependencies")
    @ApiOperation (value = "Fetch node backward dependencies")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = NodeInfo.class, responseContainer = "List"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getBackwardDependencies(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<NodeInfo> backwardDependencyNodes = storage.getBackwardDependencies(nodeId);
        return Response.ok().entity(backwardDependencyNodes).build();
    }

    @DELETE
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}")
    @ApiOperation (value = "Remove dependency to node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response removeDependency(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                     @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                     @ApiParam(value = "Name") @PathParam("name") String name,
                                     @ApiParam(value = "To Node ID") @PathParam("toNodeId") String toNodeId) {
        logInfo("Removing dependency {} ({} -> {})", name, nodeId, toNodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.removeDependency(nodeId, name, toNodeId);
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/flush")
    @ApiOperation (value = "Flush appstorage changeset")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response flush(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                          @ApiParam(value = "Storage Change Set") StorageChangeSet changeSet) {
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
    @ApiOperation (value = "Create new timeseries for given node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response createTimeSeries(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                     @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                     @ApiParam(value = "Time Series Meta Data") TimeSeriesMetadata metadata) {
        logInfo("Creating timeseries {} for node {}", metadata.getName(), nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.createTimeSeries(nodeId, metadata);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/name")
    @Compress
    @ApiOperation (value = "Fetch node timeseries")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = String.class, responseContainer = "List"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getTimeSeriesNames(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                       @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<String> timeSeriesNames = storage.getTimeSeriesNames(nodeId);
        return Response.ok().header(HttpHeaders.CONTENT_ENCODING, "gzip").entity(timeSeriesNames).build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}")
    @ApiOperation (value = "Indicate if named timeseries exist for given node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = Boolean.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response timeSeriesExists(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                     @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                     @ApiParam(value = "Time series name") @PathParam("timeSeriesName") String timeSeriesName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean exists = storage.timeSeriesExists(nodeId, timeSeriesName);
        return Response.ok().entity(exists).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/metadata")
    @Compress
    @ApiOperation (value = "Fetch timeseries metadata for given names and node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = TimeSeriesMetadata.class, responseContainer = "List"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getTimeSeriesMetadata(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                          @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                          @ApiParam(value = "Time series names") Set<String> timeSeriesNames) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(nodeId, timeSeriesNames);
        return Response.ok().header(HttpHeaders.CONTENT_ENCODING, "gzip").entity(metadataList).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/versions")
    @ApiOperation (value = "Fetch timeseries available versions")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = Integer.class, responseContainer = "List"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getTimeSeriesDataVersions(@PathParam("fileSystemName") String fileSystemName,
                                              @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<Integer> versions = storage.getTimeSeriesDataVersions(nodeId);
        return Response.ok().entity(versions).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}/versions")
    @ApiOperation (value = "Fetch timeseries available versions for given named timeseries")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = Integer.class, responseContainer = "List"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getTimeSeriesDataVersions(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                              @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                              @ApiParam(value = "Time series name") @PathParam("timeSeriesName") String timeSeriesName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<Integer> versions = storage.getTimeSeriesDataVersions(nodeId, timeSeriesName);
        return Response.ok().entity(versions).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}/{timeSeriesName}")
    @ApiOperation (value = "Append Double data chunks to name timeseries")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response addDoubleTimeSeriesData(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                            @ApiParam(value = "Version") @PathParam("version") int version,
                                            @ApiParam(value = "Time series name") @PathParam("timeSeriesName") String timeSeriesName,
                                            @ApiParam(value = "List double array chunk") List<DoubleDataChunk> chunks) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.addDoubleTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}")
    @Compress
    @ApiOperation (value = "Fetch Double data for named timeseries")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = DoubleDataChunk.class, responseContainer = "Map"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getDoubleTimeSeriesData(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                            @ApiParam(value = "Version") @PathParam("version") int version,
                                            @ApiParam(value = "Set time series names") Set<String> timeSeriesNames) {
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
    @ApiOperation (value = "Append String data chunks to named timeseries")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response addStringTimeSeriesData(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @ApiParam(value = "File system name") @PathParam("nodeId") String nodeId,
                                            @ApiParam(value = "File system name") @PathParam("version") int version,
                                            @ApiParam(value = "File system name") @PathParam("timeSeriesName") String timeSeriesName,
                                            @ApiParam(value = "List string array chunkFile system name") List<StringDataChunk> chunks) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.addStringTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/string/{version}")
    @Compress
    @ApiOperation (value = "Fetch String data for named timeseries")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = StringDataChunk.class, responseContainer = "Map"), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getStringTimeSeriesData(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                            @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId,
                                            @ApiParam(value = "Version") @PathParam("version") int version,
                                            @ApiParam(value = "Set time series names") Set<String> timeSeriesNames) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Map<String, List<StringDataChunk>> timeSeriesData = storage.getStringTimeSeriesData(nodeId, timeSeriesNames, version);
        return Response.ok()
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
                .entity(timeSeriesData)
                .build();
    }

    @DELETE
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries")
    @ApiOperation (value = "Delete all timeseries for given node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response clearTimeSeries(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                    @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        logInfo("Clearing timeseries for node {}", nodeId);
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.clearTimeSeries(nodeId);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/nodes/{nodeId}")
    @ApiOperation (value = "Fetch node info")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = NodeInfo.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response getNodeInfo(@ApiParam(value = "File system name") @PathParam("fileSystemName") String fileSystemName,
                                @ApiParam(value = "Node ID") @PathParam("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        NodeInfo nodeInfo = storage.getNodeInfo(nodeId);
        return Response.ok().entity(nodeInfo).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/tasks")
    @ApiOperation (value = "Start a task")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = TaskMonitor.Task.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response startTask(@PathParam("fileSystemName") String fileSystemName,
                              @QueryParam("projectFileId") String projectFileId,
                              @QueryParam("projectId") String projectId,
                              @QueryParam("type") String type,
                              @QueryParam("name") String name) {
        logInfo("Starting task {} for node {} ({})", name, projectFileId, projectId);
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        TaskMonitor.Task task;
        if (projectFileId != null) {
            ProjectFile projectFile = fileSystem.findProjectFile(projectFileId, ProjectFile.class);
            if (projectFile == null) {
                throw new AfsException("Project file '" + projectFileId + "' not found in file system '" + fileSystemName + "'");
            }
            task = fileSystem.getTaskMonitor().startTask(projectFile, type);
        } else if (projectId != null && name != null) {
            Project project = fileSystem
                    .findProject(projectId)
                    .orElseThrow(() -> new AfsException("Project '" + projectId + "' not found in file system '" + fileSystemName + "'"));
            task = fileSystem.getTaskMonitor().startTask(name, project, type);
        } else {
            throw new AfsException("Missing arguments");
        }
        return Response.ok().entity(task).build();
    }

    @DELETE
    @Path("fileSystems/{fileSystemName}/tasks/{taskId}")
    @ApiOperation (value = "Stop task tracking")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
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
    @ApiOperation (value = "Update a task message")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
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
    @ApiOperation (value = "Fetch current running tasks snapshot")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = TaskMonitor.Snapshot.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public Response takeSnapshot(@PathParam("fileSystemName") String fileSystemName,
                                 @QueryParam("projectId") String projectId) {
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        TaskMonitor.Snapshot snapshot = fileSystem.getTaskMonitor().takeSnapshot(projectId);
        return Response.ok().entity(snapshot).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fileSystems/{fileSystemName}/tasks/{taskId}/_cancel")
    @ApiOperation (value = "Cancel a task tracked process")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "", response = Boolean.class), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
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
