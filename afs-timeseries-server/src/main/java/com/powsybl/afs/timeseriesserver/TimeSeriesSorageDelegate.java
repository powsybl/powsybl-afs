package com.powsybl.afs.timeseriesserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.timeseries.*;
import com.powsybl.timeseries.storer.query.create.CreateQuery;
import com.powsybl.timeseries.storer.query.fetch.FetchQuery;
import com.powsybl.timeseries.storer.query.fetch.FetchQueryResult;
import com.powsybl.timeseries.storer.query.publish.PublishQuery;
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

public class TimeSeriesSorageDelegate {

    private static final String AFS_APP = "AFS";
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesSorageDelegate.class);


    private URI timeSeriesServerURI;

    public TimeSeriesSorageDelegate(URI timeSeriesServerURI) {
        this.timeSeriesServerURI = timeSeriesServerURI;
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
            if (apps.contains(AFS_APP)) {
                return;
            }

            response = buildBaseRequest(client).path(AFS_APP).request().post(Entity.json(""));
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
                .path(AFS_APP)
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
                .path(AFS_APP)
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

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setMatrix(nodeId);
        searchQuery.setNames(Collections.singleton(name));
        SearchQueryResults results = performSearch(searchQuery);
        if (results != null) {
            return results.getTimeSeriesInformations().size() > 0;
        }
        return false;
    }

    public List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setMatrix(nodeId);
        searchQuery.setNames(timeSeriesNames);
        SearchQueryResults results = performSearch(searchQuery);
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

    public void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {

        //First step : retrieve metadata and reconstitute the time series
        TimeSeriesMetadata metadata = getTimeSeriesMetadata(nodeId, Collections.singleton(timeSeriesName)).get(0);
        StoredDoubleTimeSeries ts = new StoredDoubleTimeSeries(metadata, chunks);

        //Second step : perform a publish request
        PublishQuery<Double> publishQuery = new PublishQuery<>();
        publishQuery.setMatrix(nodeId);
        publishQuery.setTimeSeriesName(timeSeriesName);
        publishQuery.setVersionName(String.valueOf(version));
        publishQuery.setData(ArrayUtils.toObject(ts.toArray()));

        Client client = createClient();
        try {
            Response response = buildBaseRequest(client)
                .path(AFS_APP)
                .path("series")
                .request().put(Entity.json(new ObjectMapper().writeValueAsString(publishQuery)));
            if (response.getStatus() != 200) {
                throw new AfsStorageException("Error while publishing data to time series server");
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            client.close();
        }

    }

    public Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {

        String versionString = String.valueOf(version);
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setMatrix(nodeId);
        searchQuery.setNames(timeSeriesNames);
        SearchQueryResults results = performSearch(searchQuery);
        List<Long> versionIDs = results.getTimeSeriesInformations()
            .stream()
            .map(t->t.getVersions().get(versionString))
            .collect(Collectors.toList());

        Map<Long, String> versionIDToTSName = results.getTimeSeriesInformations()
            .stream()
            .collect(Collectors.toMap(t->t.getVersions().get(versionString), t->t.getName()));

        FetchQuery query = new FetchQuery(versionIDs, null, null);
        Client client = createClient();
        try {
            Response response = buildBaseRequest(client)
                .path(AFS_APP)
                .path("series")
                .path("_fetch")
                .request().post(Entity.json(new ObjectMapper().writeValueAsString(query)));
            if (response.getStatus() != 200) {
                throw new AfsStorageException("Error while fetching data from time series server");
            }
            String json = response.readEntity(String.class);
            FetchQueryResult<Double> fetchResults = new ObjectMapper().readValue(json, FetchQueryResult.class);

            Map<String, List<DoubleDataChunk>> toReturn = new HashMap<>();
            for(int i=0; i<versionIDs.size(); i++)
            {
                double[] values = fetchResults.getData().get(i).stream().mapToDouble(Double::doubleValue).toArray();
                UncompressedDoubleDataChunk chunk = new UncompressedDoubleDataChunk(0, values);
                toReturn.put(versionIDToTSName.get(versionIDs.get(i)), Arrays.asList(chunk));
            }

        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            client.close();
        }

        return null;
    }
}
