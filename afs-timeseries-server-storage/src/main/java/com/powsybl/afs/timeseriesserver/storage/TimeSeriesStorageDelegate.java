package com.powsybl.afs.timeseriesserver.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.timeseries.*;
import com.powsybl.timeseries.storer.query.create.CreateQuery;
import com.powsybl.timeseries.storer.query.fetch.FetchQuery;
import com.powsybl.timeseries.storer.query.fetch.result.FetchQueryDoubleResult;
import com.powsybl.timeseries.storer.query.fetch.result.FetchQueryStringResult;
import com.powsybl.timeseries.storer.query.publish.PublishDoubleQuery;
import com.powsybl.timeseries.storer.query.publish.AbstractPublishQuery;
import com.powsybl.timeseries.storer.query.publish.PublishStringQuery;
import com.powsybl.timeseries.storer.query.search.SearchQuery;
import com.powsybl.timeseries.storer.query.search.SearchQueryResults;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class TimeSeriesStorageDelegate {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesStorageDelegate.class);

    /**
     * The name of the app in TimeSeriesServer, in which AFS will store time series
     */
    private final String app;

    /**
     * The address of the TimeSeriesServer
     */
    private URI timeSeriesServerURI;

    public TimeSeriesStorageDelegate(URI timeSeriesServerURI, String app) {
        this.timeSeriesServerURI = timeSeriesServerURI;
        this.app = app;
    }

    public static Client createClient() {
        return new ResteasyClientBuilder()
                    .connectionPoolSize(50)
                    .build();
    }

    private WebTarget buildBaseRequest(Client client) {
        return client.target(timeSeriesServerURI)
                     .path("v1")
                     .path("timeseries")
                     .path("apps");
    }

    public void createAFSAppIfNotExists() {
        Client client = createClient();
        try {
            Response response = buildBaseRequest(client).request().get();

            Collection<String> apps = response.readEntity(Collection.class);
            if (apps.contains(app)) {
                return;
            }

            response = buildBaseRequest(client).path(app).request().post(Entity.json(""));
            if (response.getStatus() != 200) {
                throw new AfsStorageException("Error while initializing AFS timeseries app storage");
            }

        } finally {
            client.close();
        }

    }

    public void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        if (!(metadata.getIndex() instanceof RegularTimeSeriesIndex)) {
            throw new NotImplementedException("TimeSeriesServer only handles regular time series for now.");
        }
        RegularTimeSeriesIndex index = (RegularTimeSeriesIndex) metadata.getIndex();

        CreateQuery createQuery = new CreateQuery();
        createQuery.setMatrix(nodeId);
        createQuery.setName(metadata.getName());
        createQuery.setTags(metadata.getTags());
        createQuery.setTimeStepCount(index.getPointCount());
        createQuery.setTimeStepDuration(index.getSpacing());
        LocalDateTime startDate = Instant.ofEpochMilli(index.getStartTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        createQuery.setStartDate(startDate);

        Client client = createClient();
        try {
            buildBaseRequest(client)
                        .path(app)
                        .path("series")
                        .request().post(Entity.json(new ObjectMapper().writeValueAsString(createQuery)));
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            client.close();
        }
    }

    public SearchQueryResults performSearch(SearchQuery query) {

        SearchQueryResults results = null;

        Client client = createClient();
        try {
            Response response = buildBaseRequest(client)
                        .path(app)
                        .path("series")
                        .path("_search")
                        .request().post(Entity.json(new ObjectMapper().writeValueAsString(query)));
            String json = response.readEntity(String.class);
            results = new ObjectMapper().readValue(json, SearchQueryResults.class);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            client.close();
        }

        return results;
    }

    public Set<String> getTimeSeriesNames(String nodeId) {

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setMatrix(nodeId);

        SearchQueryResults results = performSearch(searchQuery);
        if (results != null) {
            return results.getTimeSeriesInformations()
                          .stream().map(t -> t.getName()).collect(Collectors.toSet());
        }
        return null;
    }

    public boolean timeSeriesExists(String nodeId, String name) {

        SearchQueryResults results = doSearch(nodeId, Collections.singleton(name));
        if (results != null) {
            return results.getTimeSeriesInformations().size() > 0;
        }
        return false;
    }

    public List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {

        SearchQueryResults results = doSearch(nodeId, timeSeriesNames);
        if (results != null) {
            return results.getTimeSeriesInformations()
                          .stream().map(t -> {
                            long startTime = t.getStartDate().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            long spacing = t.getTimeStepDuration();
                            long endTime = startTime + spacing * (t.getTimeStepCount() - 1);
                            TimeSeriesIndex index = new RegularTimeSeriesIndex(startTime, endTime, spacing);
                            return new TimeSeriesMetadata(t.getName(), TimeSeriesDataType.DOUBLE, t.getTags(), index);
                        })
                          .collect(Collectors.toList());
        }
        return null;
    }

    public Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setMatrix(nodeId);
        if (timeSeriesName != null) {
            searchQuery.setNames(Collections.singleton(timeSeriesName));
        }
        SearchQueryResults results = performSearch(searchQuery);
        if (results != null) {
            return results.getTimeSeriesInformations()
                          .stream().flatMap(t -> t.getVersions().keySet().stream())
                          .map(t -> Integer.parseInt(t))
                          .collect(Collectors.toSet());
        }
        return null;
    }

    /**
     * Add a new string time series
     *
     * @param nodeId         identifier for the node
     * @param version        version to publish to
     * @param timeSeriesName name of the time series
     * @param chunks         actual (string) data to publish
     */
    public void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {
        // Retrieve metadata and build time series
        TimeSeriesMetadata metadata = getTimeSeriesMetadata(nodeId, Collections.singleton(timeSeriesName)).get(0);
        StringTimeSeries ts = new StringTimeSeries(metadata, chunks);

        // Prepare publish request
        AbstractPublishQuery<String> publishQuery = new PublishStringQuery();
        publishQuery.setMatrix(nodeId);
        publishQuery.setTimeSeriesName(timeSeriesName);
        publishQuery.setVersionName(String.valueOf(version));
        publishQuery.setData(ts.toArray());

        // Do run query
        doPublish(publishQuery);
    }

    /**
     * Add a new double time series
     *
     * @param nodeId         identifier for the node
     * @param version        version to publish to
     * @param timeSeriesName name of the time series
     * @param chunks         actual (double) data to publish
     */
    public void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {

        //First step : retrieve metadata and reconstitute the time series
        TimeSeriesMetadata metadata = getTimeSeriesMetadata(nodeId, Collections.singleton(timeSeriesName)).get(0);
        StoredDoubleTimeSeries ts = new StoredDoubleTimeSeries(metadata, chunks);

        //Second step : perform a publish request
        AbstractPublishQuery<Double> publishQuery = new PublishDoubleQuery();
        publishQuery.setMatrix(nodeId);
        publishQuery.setTimeSeriesName(timeSeriesName);
        publishQuery.setVersionName(String.valueOf(version));
        publishQuery.setData(ArrayUtils.toObject(ts.toArray()));

        // Do run query
        doPublish(publishQuery);
    }

    /**
     * Retrieve time series String data from server
     * Perform a search query with provided criteria, followed by a fetch query (using search results)
     * @param nodeId          identifier for the node to retrieve data for
     * @param timeSeriesNames names of time series to retrieve
     * @param version         version of the data to fetch
     * @return a map contaning all queried time series, indexed by their name
     */
    public Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        String versionString = String.valueOf(version);
        // First perform a search query
        final SearchQueryResults searchResults = doSearch(nodeId, timeSeriesNames);
        List<Long> versionIDs = searchResults.getVersionIds(versionString);
        Map<Long, String> versionToName = searchResults.getVersionToNameMap(versionString);
        // Then perform the fetch query
        final Response response = doFetch(versionToName);
        // Extract fetch result
        String json = response.readEntity(String.class);
        FetchQueryStringResult fetchResults;
        try {
            fetchResults = new ObjectMapper().readValue(json, FetchQueryStringResult.class);
        } catch (JsonProcessingException e) {
            throw new AfsStorageException("Could not process fetch query result into a String query result");
        }
        Map<String, List<StringDataChunk>> result = new HashMap<>();
        for (int i = 0; i < versionToName.keySet().size(); i++) {
            String[] values = fetchResults.getData().get(i).toArray(new String[0]);
            StringDataChunk chunk = new UncompressedStringDataChunk(0, values);
            result.put(versionToName.get(versionIDs.get(i)), Collections.singletonList(chunk));
        }
        return result;
    }

    /**
     * Perform search then fetch queries to retrieve double data against time series server
     *
     * @param nodeId          identifier for the node to retrieve time series for
     * @param timeSeriesNames names of time series to search for
     * @param version         version of the data to fetch
     * @return time series names, mapped to their respective results (as chunks)
     */
    public Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        String versionString = String.valueOf(version);
        // First perform a search query
        final SearchQueryResults searchResults = doSearch(nodeId, timeSeriesNames);
        List<Long> versionIDs = searchResults.getVersionIds(versionString);
        Map<Long, String> versionToName = searchResults.getVersionToNameMap(versionString);
        // Then perform the fetch query
        final Response response = doFetch(versionToName);
        // Extract fetch result
        String json = response.readEntity(String.class);
        FetchQueryDoubleResult fetchResults;
        try {
            fetchResults = new ObjectMapper().readValue(json, FetchQueryDoubleResult.class);
        } catch (JsonProcessingException e) {
            throw new AfsStorageException("Could not process fetch query result into a String query result");
        }
        // Extract data from results
        Map<String, List<DoubleDataChunk>> toReturn = new HashMap<>();
        for (int i = 0; i < versionIDs.size(); i++) {
            double[] values = fetchResults.getData().get(i).stream().mapToDouble(Double::doubleValue).toArray();
            UncompressedDoubleDataChunk chunk = new UncompressedDoubleDataChunk(0, values);
            toReturn.put(versionToName.get(versionIDs.get(i)), Collections.singletonList(chunk));
        }
        return toReturn;
    }

    /**
     * Perform search request against time series server
     *
     * @param nodeId          identifier of the node to search for
     * @param timeSeriesNames names of time series to search for
     * @return a SearchQueryResults object representing the TS server response
     */
    private SearchQueryResults doSearch(final String nodeId, final Set<String> timeSeriesNames) {
        // Prepare search query
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setMatrix(nodeId);
        searchQuery.setNames(timeSeriesNames);
        // Run search query
        return performSearch(searchQuery);
    }

    /**
     * Perform a search query against distant API
     *
     * @param versionToName a map of TS version -> TS name
     * @return fetch HTTP response
     */
    private Response doFetch(final Map<Long, String> versionToName) {
        // Prepare fetch query
        FetchQuery query = new FetchQuery(versionToName.keySet(), null, null);
        Client client = createClient();
        try {
            Response response = buildBaseRequest(client)
                        .path(app)
                        .path("series")
                        .path("_fetch")
                        .request().post(Entity.json(new ObjectMapper().writeValueAsString(query)));
            if (response.getStatus() != 200) {
                throw new AfsStorageException("Error while fetching data from time series server");
            }
            return response;
        } catch (JsonProcessingException e) {
            throw new AfsStorageException("Error while fetching data from time series server");
        } finally {
            client.close();
        }
    }

    /**
     * Perform a publish query
     *
     * @param publishQuery query to issue
     */
    private void doPublish(final AbstractPublishQuery<?> publishQuery) {
        // Run request
        Client client = createClient();
        try {
            Response response = buildBaseRequest(client)
                        .path(app)
                        .path("series")
                        .request()
                        .put(Entity.json(new ObjectMapper().writeValueAsString(publishQuery)));
            if (response.getStatus() != 200) {
                throw new AfsStorageException("Error while publishing data to time series server");
            }
        } catch (IOException ioe) {
            LOGGER.error("Could not publish String time series", ioe);
        } finally {
            client.close();
        }
    }
}
