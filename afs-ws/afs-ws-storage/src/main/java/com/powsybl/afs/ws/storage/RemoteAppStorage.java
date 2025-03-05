/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.buffer.StorageChangeBuffer;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;
import com.powsybl.afs.ws.client.utils.ClientUtils;
import com.powsybl.afs.ws.storage.websocket.WebsocketConnectionPolicy;
import com.powsybl.afs.ws.utils.AfsRestApi;
import com.powsybl.afs.ws.utils.JsonProvider;
import com.powsybl.afs.ws.utils.gzip.ReaderInterceptorGzip;
import com.powsybl.afs.ws.utils.gzip.WriterInterceptorGzipCli;
import com.powsybl.commons.exceptions.UncheckedInterruptedException;
import com.powsybl.commons.io.ForwardingInputStream;
import com.powsybl.commons.io.ForwardingOutputStream;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import com.powsybl.timeseries.TimeSeriesVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.client.AsyncInvoker;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.*;
import java.io.*;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.powsybl.afs.ws.client.utils.ClientUtils.*;

/**
 * @author Ali Tahanout {@literal <ali.tahanout at rte-france.com>}
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class RemoteAppStorage extends AbstractAppStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteAppStorage.class);

    private static final int BUFFER_MAXIMUM_CHANGE = 1000;
    private static final long BUFFER_MAXIMUM_SIZE = Math.round(Math.pow(2, 20)); // 1Mo
    private static final String FILE_SYSTEM_NAME = "fileSystemName";
    private static final String NODE_ID = "nodeId";
    private static final String VERSION = "version";
    private static final String NODE_DATA_PATH = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}";

    private final Client client;

    private final WebTarget webTarget;

    private final String fileSystemName;

    private final StorageChangeBuffer changeBuffer;

    private final String token;

    private boolean closed = false;

    public RemoteAppStorage(String fileSystemName, URI baseUri) {
        this(fileSystemName, baseUri, "");
    }

    public RemoteAppStorage(String fileSystemName, URI baseUri, String token) {
        this(fileSystemName, baseUri, token, WebsocketConnectionPolicy.standard());
    }

    public RemoteAppStorage(String fileSystemName, URI baseUri, String token, WebsocketConnectionPolicy websocketConnectionPolicy) {
        this.fileSystemName = Objects.requireNonNull(fileSystemName);
        this.token = token;
        this.eventsBus = new WebSocketEventsBus(this, baseUri, websocketConnectionPolicy);

        client = createClient();

        webTarget = getWebTarget(client, baseUri)
                .register(WriterInterceptorGzipCli.class)
                .register(ReaderInterceptorGzip.class);

        changeBuffer = new StorageChangeBuffer(changeSet -> {
            LOGGER.debug("flush(fileSystemName={}, size={})", fileSystemName, changeSet.getChanges().size());

            try (Response response = webTarget.path("fileSystems/{fileSystemName}/flush")
                .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
                .request()
                .header(HttpHeaders.AUTHORIZATION, token)
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
                .acceptEncoding("gzip")
                .post(Entity.json(changeSet))) {
                checkOk(response);
            }
        }, BUFFER_MAXIMUM_CHANGE, BUFFER_MAXIMUM_SIZE);
    }

    static Client createClient() {
        return ClientUtils.createClient()
                .register(new JsonProvider());
    }

    static WebTarget getWebTarget(Client client, URI baseUri) {
        return client.target(baseUri)
                .path("rest")
                .path(AfsRestApi.RESOURCE_ROOT)
                .path(AfsRestApi.VERSION);
    }

    @Override
    public String getFileSystemName() {
        return fileSystemName;
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    public static List<String> getFileSystemNames(URI baseUri, String token) {
        try (Client client = createClient()) {
            try (Response response = getWebTarget(client, baseUri)
                .path("fileSystems")
                .request(MediaType.APPLICATION_JSON)
                .header(HttpHeaders.AUTHORIZATION, token)
                .get()) {
                List<String> fileSystemNames = response.readEntity(new GenericType<>() {
                });
                LOGGER.info("File systems {} found at {}", fileSystemNames, baseUri);
                return fileSystemNames;
            }
        }
    }

    @Override
    public NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(nodePseudoClass);
        LOGGER.debug("createRootNodeIfNotExists(fileSystemName={}, name={}, nodePseudoClass={})",
                fileSystemName, name, nodePseudoClass);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/rootNode")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .queryParam("nodeName", name)
            .queryParam("nodePseudoClass", nodePseudoClass)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .put(Entity.text(""))) {
            return readEntityIfOk(response, NodeInfo.class);
        }
    }

    @Override
    public boolean isWritable(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("isWritable(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/writable")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request(MediaType.TEXT_PLAIN)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, Boolean.class);
        }
    }

    @Override
    public boolean isConsistent(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("isConsistent(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/consistent")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request(MediaType.TEXT_PLAIN)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, Boolean.class);
        }
    }

    @Override
    public void setDescription(String nodeId, String description) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(description);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("setDescription(fileSystemName={}, nodeId={}, description={})", fileSystemName, nodeId, description);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/description")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .acceptEncoding("gzip")
            .put(Entity.text(description))) {
            checkOk(response);
        }
    }

    @Override
    public void setConsistent(String nodeId) {
        Objects.requireNonNull(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("setConsistent(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/consistent")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .acceptEncoding("gzip")
            .put(Entity.json(true))) {
            checkOk(response);
        }
    }

    @Override
    public void renameNode(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("renameNode(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/name")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .acceptEncoding("gzip")
            .put(Entity.text(name))) {
            checkOk(response);
        }
    }

    @Override
    public void updateModificationTime(String nodeId) {
        Objects.requireNonNull(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("updateModificationTime(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/modificationTime")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .acceptEncoding("gzip")
            .put(Entity.text(""))) {
            checkOk(response);
        }
    }

    @Override
    public NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, int version,
                               NodeGenericMetadata genericMetadata) {
        Objects.requireNonNull(parentNodeId);
        Objects.requireNonNull(name);
        Objects.requireNonNull(nodePseudoClass);
        Objects.requireNonNull(description);
        Objects.requireNonNull(genericMetadata);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("createNode(fileSystemName={}, parentNodeId={}, name={}, nodePseudoClass={}, description={}, version={}, genericMetadata={})",
                fileSystemName, parentNodeId, name, nodePseudoClass, description, version, genericMetadata);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, parentNodeId)
            .resolveTemplate("childName", name)
            .queryParam("nodePseudoClass", nodePseudoClass)
            .queryParam("description", description)
            .queryParam(VERSION, version)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .acceptEncoding("gzip")
            .post(Entity.json(genericMetadata))) {
            return readEntityIfOk(response, NodeInfo.class);
        }
    }

    @Override
    public void setMetadata(String nodeId, NodeGenericMetadata genericMetadata) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(genericMetadata);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("setMetadata(fileSystemName={}, nodeId={}, genericMetadata={})", fileSystemName, nodeId, genericMetadata);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/metadata")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .acceptEncoding("gzip")
            .put(Entity.json(genericMetadata))) {
            checkOk(response);
        }
    }

    @Override
    public List<NodeInfo> getChildNodes(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getChildNodes(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/children")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public List<NodeInfo> getInconsistentNodes() {
        LOGGER.debug("getInconsistentNodes(fileSystemName={})", fileSystemName);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/inconsistentChildNodes")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public Optional<NodeInfo> getChildNode(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("getChildNode(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate("childName", name)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readOptionalEntityIfOk(response, NodeInfo.class);
        }
    }

    @Override
    public Optional<NodeInfo> getParentNode(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getParentNode(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/parent")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readOptionalEntityIfOk(response, NodeInfo.class);
        }
    }

    @Override
    public void setParentNode(String nodeId, String newParentNodeId) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(newParentNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("setParentNode(fileSystemName={}, nodeId={}, newParentNodeId={})", fileSystemName, nodeId, newParentNodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/parent")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .acceptEncoding("gzip")
            .put(Entity.text(newParentNodeId))) {
            checkOk(response);
        }
    }

    @Override
    public String deleteNode(String nodeId) {
        Objects.requireNonNull(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("deleteNode(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .delete()) {
            return readEntityIfOk(response, String.class);
        }
    }

    private static class OutputStreamPutRequest extends ForwardingOutputStream<PipedOutputStream> {

        private final Future<Response> response;

        public OutputStreamPutRequest(AsyncInvoker asyncInvoker) {
            super(new PipedOutputStream());
            Objects.requireNonNull(asyncInvoker);

            PipedInputStream pis;
            try {
                pis = new PipedInputStream(os);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            StreamingOutput output = os -> ByteStreams.copy(pis, os);

            response = asyncInvoker.put(Entity.entity(output, MediaType.APPLICATION_OCTET_STREAM));
        }

        @Override
        public void close() throws IOException {
            super.close();

            // check the request status after the stream is closed
            try {
                checkOk(response.get());
            } catch (ExecutionException e) {
                throw new UncheckedExecutionException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedInterruptedException(e);
            }
        }
    }

    @Override
    public Optional<InputStream> readBinaryData(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("readBinaryData(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        Response response = webTarget.path(NODE_DATA_PATH)
                .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
                .resolveTemplate(NODE_ID, nodeId)
                .resolveTemplate("name", name)
                .request(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.AUTHORIZATION, token)
                .get();
        return readOptionalEntityIfOk(response, InputStream.class)
                .map(is -> new ForwardingInputStream<>(is) {
                    @Override
                    public void close() throws IOException {
                        super.close();

                        response.close();
                    }
                });
    }

    @Override
    public OutputStream writeBinaryData(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("writeBinaryData(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        AsyncInvoker asyncInvoker = webTarget.path(NODE_DATA_PATH)
                .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
                .resolveTemplate(NODE_ID, nodeId)
                .resolveTemplate("name", name)
                .request()
                .header(HttpHeaders.AUTHORIZATION, token)
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
                .acceptEncoding("gzip")
                .async();

        return new OutputStreamPutRequest(asyncInvoker);
    }

    @Override
    public boolean dataExists(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("dataExists(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        try (Response response = webTarget.path(NODE_DATA_PATH)
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate("name", name)
            .request(MediaType.TEXT_PLAIN)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, Boolean.class);
        }
    }

    @Override
    public Set<String> getDataNames(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getDataNames(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/data")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public boolean removeData(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("removeData(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        try (Response response = webTarget.path(NODE_DATA_PATH)
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate("name", name)
            .request(MediaType.TEXT_PLAIN)
            .header(HttpHeaders.AUTHORIZATION, token)
            .delete()) {
            return readEntityIfOk(response, Boolean.class);
        }
    }

    @Override
    public void addDependency(String nodeId, String name, String toNodeId) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);
        Objects.requireNonNull(toNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("addDependency(fileSystemName={}, nodeId={}, name={}, toNodeId={})", fileSystemName, nodeId, name, toNodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate("name", name)
            .resolveTemplate("toNodeId", toNodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .header(HttpHeaders.CONTENT_ENCODING, "gzip")
            .acceptEncoding("gzip")
            .put(Entity.text(""))) {
            checkOk(response);
        }
    }

    @Override
    public Set<NodeInfo> getDependencies(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("getDependencies(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate("name", name)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public Set<NodeDependency> getDependencies(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getDependencies(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public Set<NodeInfo> getBackwardDependencies(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getBackwardDependencies(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/backwardDependencies")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public void removeDependency(String nodeId, String name, String toNodeId) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);
        Objects.requireNonNull(toNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("removeDependency(fileSystemName={}, nodeId={}, name={}, toNodeId={})", fileSystemName, nodeId, name, toNodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate("name", name)
            .resolveTemplate("toNodeId", toNodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .delete()) {
            checkOk(response);
        }
    }

    @Override
    public void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(metadata);

        LOGGER.debug("createTimeSeries(fileSystemName={}, nodeId={}, metadata={}) [BUFFERED]", fileSystemName, nodeId, metadata);

        changeBuffer.createTimeSeries(nodeId, metadata);
    }

    @Override
    public Set<String> getTimeSeriesNames(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getTimeSeriesNames(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/name")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesName);

        LOGGER.debug("timeSeriesExists(fileSystemName={}, nodeId={}, timeSeriesName={})", fileSystemName, nodeId, timeSeriesName);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate("timeSeriesName", timeSeriesName)
            .request(MediaType.TEXT_PLAIN_TYPE)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, Boolean.class);
        }
    }

    @Override
    public List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesNames);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("getTimeSeriesMetadata(fileSystemName={}, nodeId={}, timeSeriesNames={})", fileSystemName, nodeId, timeSeriesNames);
        }

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/metadata")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .post(Entity.json(timeSeriesNames))) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getTimeSeriesDataVersions(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/versions")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesName);

        LOGGER.debug("getTimeSeriesDataVersions(fileSystemName={}, nodeId={}, timeSeriesNames={})", fileSystemName, nodeId, timeSeriesName);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}/versions")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate("timeSeriesName", timeSeriesName)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {
        Objects.requireNonNull(nodeId);
        TimeSeriesVersions.check(version);
        Objects.requireNonNull(timeSeriesName);
        Objects.requireNonNull(chunks);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("addDoubleTimeSeriesData(fileSystemName={}, nodeId={}, version={}, timeSeriesName={}, chunks={}) [BUFFERED]",
                    fileSystemName, nodeId, version, timeSeriesName, chunks);
        }

        changeBuffer.addDoubleTimeSeriesData(nodeId, version, timeSeriesName, chunks);
    }

    @Override
    public Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesNames);
        TimeSeriesVersions.check(version);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("getDoubleTimeSeriesData(fileSystemName={}, nodeId={}, timeSeriesNames={}, version={})",
                    fileSystemName, nodeId, timeSeriesNames, version);
        }

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate(VERSION, version)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .post(Entity.json(timeSeriesNames))) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {
        Objects.requireNonNull(nodeId);
        TimeSeriesVersions.check(version);
        Objects.requireNonNull(timeSeriesName);
        Objects.requireNonNull(chunks);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("addStringTimeSeriesData(fileSystemName={}, nodeId={}, version={}, timeSeriesName={}, chunks={}) [BUFFERED]",
                    fileSystemName, nodeId, version, timeSeriesName, chunks);
        }

        changeBuffer.addStringTimeSeriesData(nodeId, version, timeSeriesName, chunks);
    }

    @Override
    public Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesNames);
        TimeSeriesVersions.check(version);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("getStringTimeSeriesData(fileSystemName={}, nodeId={}, timeSeriesNames={}, version={})",
                    fileSystemName, nodeId, timeSeriesNames, version);
        }

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/string/{version}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .resolveTemplate(VERSION, version)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .post(Entity.json(timeSeriesNames))) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public void clearTimeSeries(String nodeId) {
        Objects.requireNonNull(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("clearTimeSeries(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request()
            .header(HttpHeaders.AUTHORIZATION, token)
            .delete()) {
            checkOk(response);
        }
    }

    @Override
    public NodeInfo getNodeInfo(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getNodeInfo(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        try (Response response = webTarget.path("fileSystems/{fileSystemName}/nodes/{nodeId}")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
            .resolveTemplate(NODE_ID, nodeId)
            .request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, NodeInfo.class);
        }
    }

    @Override
    public List<String> getSupportedFileSystemChecks() {
        WebTarget target = webTarget.path("fileSystems/{fileSystemName}/check/types")
            .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName);
        try (Response response = target.request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .get()) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public List<FileSystemCheckIssue> checkFileSystem(FileSystemCheckOptions options) {
        Long expirationMillis = options.getInconsistentNodesExpirationTime()
            .map(Instant::toEpochMilli)
            .orElse(null);
        WebTarget target = webTarget.path("fileSystems/{fileSystemName}/check")
                .resolveTemplate(FILE_SYSTEM_NAME, fileSystemName)
                .queryParam("repair", options.isRepair())
                .queryParam("instant", expirationMillis)
                .queryParam("types", String.join(",", options.getTypes()));

        try (Response response = target.request(MediaType.APPLICATION_JSON)
            .header(HttpHeaders.AUTHORIZATION, token)
            .post(Entity.json(""))) {
            return readEntityIfOk(response, new GenericType<>() {
            });
        }
    }

    @Override
    public void flush() {
        changeBuffer.flush();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (!closed) {
            flush();
            eventsBus.close();
            client.close();
            closed = true;
        }
    }
}
