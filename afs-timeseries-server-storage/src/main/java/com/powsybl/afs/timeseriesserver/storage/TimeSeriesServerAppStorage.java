package com.powsybl.afs.timeseriesserver.storage;

import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.events.AppStorageListener;
import com.powsybl.afs.storage.events.TimeSeriesCreated;
import com.powsybl.afs.storage.events.TimeSeriesDataUpdated;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import org.apache.commons.lang3.NotImplementedException;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TimeSeriesServerAppStorage extends AbstractAppStorage {

    @FunctionalInterface
    public interface TimeSeriesServerAppStorageProvider<F, S, T, R> {
        R apply(F first, S second, T third);
    }

    /**
     * This storage is used for all non-timeseries-related operations
     */
    private AbstractAppStorage generalDelegate;

    /**
     * This storage handles all the timeseries-related operations
     */
    private TimeSeriesStorageDelegate timeSeriesDelegate;

    /**
     * A listener that copies all event from the general delegate event bus to this class event bus.
     * This has to be a field because the listeners of an event bus are stored in a WeakReferenceList
     */
    private AppStorageListener notifyGeneralDelegateEventListener;

    public TimeSeriesServerAppStorage(final URI targetURI, final String app, final AbstractAppStorage generalDelegate) {
        this(generalDelegate, targetURI, app);
    }

    public TimeSeriesServerAppStorage(final AbstractAppStorage generalDelegate, final URI timeSeriesServerURI, final String app) {
        this.generalDelegate = generalDelegate;
        eventsBus = new InMemoryEventsBus();
        notifyGeneralDelegateEventListener = t -> t.getEvents().forEach(e -> pushEvent(e, t.getTopic()));
        generalDelegate.getEventsBus().addListener(notifyGeneralDelegateEventListener);
        timeSeriesDelegate = new TimeSeriesStorageDelegate(timeSeriesServerURI, app);
        timeSeriesDelegate.createAFSAppIfNotExists();
    }

    @Override
    public String getFileSystemName() {
        return generalDelegate.getFileSystemName();
    }

    @Override
    public boolean isRemote() {
        return generalDelegate.isRemote();
    }

    @Override
    public NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        return generalDelegate.createRootNodeIfNotExists(name, nodePseudoClass);
    }

    @Override
    public NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description, int version, NodeGenericMetadata genericMetadata) {
        return generalDelegate.createNode(parentNodeId, name, nodePseudoClass, description, version, genericMetadata);
    }

    @Override
    public boolean isWritable(String nodeId) {
        return generalDelegate.isWritable(nodeId);
    }

    @Override
    public NodeInfo getNodeInfo(String nodeId) {
        return generalDelegate.getNodeInfo(nodeId);
    }

    @Override
    public void setDescription(String nodeId, String description) {
        generalDelegate.setDescription(nodeId, description);
    }

    @Override
    public void updateModificationTime(String nodeId) {
        generalDelegate.updateModificationTime(nodeId);
    }

    @Override
    public List<NodeInfo> getChildNodes(String nodeId) {
        return generalDelegate.getChildNodes(nodeId);
    }

    @Override
    public Optional<NodeInfo> getChildNode(String nodeId, String name) {
        return generalDelegate.getChildNode(nodeId, name);
    }

    @Override
    public Optional<NodeInfo> getParentNode(String nodeId) {
        return generalDelegate.getParentNode(nodeId);
    }

    @Override
    public void setParentNode(String nodeId, String newParentNodeId) {
        generalDelegate.setParentNode(nodeId, newParentNodeId);
    }

    @Override
    public String deleteNode(String nodeId) {
        return generalDelegate.deleteNode(nodeId);
    }

    @Override
    public Optional<InputStream> readBinaryData(String nodeId, String name) {
        return generalDelegate.readBinaryData(nodeId, name);
    }

    @Override
    public OutputStream writeBinaryData(String nodeId, String name) {
        return generalDelegate.writeBinaryData(nodeId, name);
    }

    @Override
    public boolean dataExists(String nodeId, String name) {
        return generalDelegate.dataExists(nodeId, name);
    }

    @Override
    public Set<String> getDataNames(String nodeId) {
        return generalDelegate.getDataNames(nodeId);
    }

    @Override
    public boolean removeData(String nodeId, String name) {
        return generalDelegate.removeData(nodeId, name);
    }

    @Override
    public void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        timeSeriesDelegate.createTimeSeries(nodeId, metadata);
        pushEvent(new TimeSeriesCreated(nodeId, metadata.getName()), AbstractAppStorage.APPSTORAGE_TIMESERIES_TOPIC);
    }

    @Override
    public Set<String> getTimeSeriesNames(String nodeId) {
        return timeSeriesDelegate.getTimeSeriesNames(nodeId);
    }

    @Override
    public boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        return timeSeriesDelegate.timeSeriesExists(nodeId, timeSeriesName);
    }

    @Override
    public List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {
        return timeSeriesDelegate.getTimeSeriesMetadata(nodeId, timeSeriesNames);
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        return timeSeriesDelegate.getTimeSeriesDataVersions(nodeId, null);
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        return timeSeriesDelegate.getTimeSeriesDataVersions(nodeId, timeSeriesName);
    }

    @Override
    public Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        //TODO
        return timeSeriesDelegate.getDoubleTimeSeriesData(nodeId, timeSeriesNames, version);
    }

    @Override
    public void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {
        timeSeriesDelegate.addDoubleTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        pushEvent(new TimeSeriesDataUpdated(nodeId, timeSeriesName), AbstractAppStorage.APPSTORAGE_TIMESERIES_TOPIC);
    }

    @Override
    public Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        throw new NotImplementedException("Not implemented in V1");
    }

    @Override
    public void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {
        throw new NotImplementedException("Not implemented in V1");
    }

    @Override
    public void clearTimeSeries(String nodeId) {
        //TODO
    }

    @Override
    public void addDependency(String nodeId, String name, String toNodeId) {
        generalDelegate.addDependency(nodeId, name, toNodeId);
    }

    @Override
    public Set<NodeInfo> getDependencies(String nodeId, String name) {
        return generalDelegate.getDependencies(nodeId, name);
    }

    @Override
    public Set<NodeDependency> getDependencies(String nodeId) {
        return generalDelegate.getDependencies(nodeId);
    }

    @Override
    public Set<NodeInfo> getBackwardDependencies(String nodeId) {
        return generalDelegate.getBackwardDependencies(nodeId);
    }

    @Override
    public void removeDependency(String nodeId, String name, String toNodeId) {
        generalDelegate.removeDependency(nodeId, name, toNodeId);
    }

    @Override
    public void flush() {
        generalDelegate.flush();
        eventsBus.flush();
    }

    @Override
    public boolean isClosed() {
        return generalDelegate.isClosed();
    }

    @Override
    public void close() {
        generalDelegate.close();
    }

    @Override
    public boolean isConsistent(String nodeId) {
        return generalDelegate.isConsistent(nodeId);
    }

    @Override
    public void setMetadata(String nodeId, NodeGenericMetadata genericMetadata) {
        generalDelegate.setMetadata(nodeId, genericMetadata);
    }

    @Override
    public void setConsistent(String nodeId) {
        generalDelegate.setConsistent(nodeId);
    }

    @Override
    public List<NodeInfo> getInconsistentNodes() {
        return generalDelegate.getInconsistentNodes();
    }

    @Override
    public void renameNode(String nodeId, String name) {
        generalDelegate.renameNode(nodeId, name);
    }
}
