package com.zuhlke.ta.prototype.solutions.couchbase;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.ResultsStore;
import com.zuhlke.ta.prototype.SentimentTimeline;
import com.couchbase.client.java.*;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CouchbaseResultsStore implements ResultsStore {
    private static final String BucketName = "results";
    private static final String DesignName = "results";
    private static final String ViewName = "all";

    private final Cluster cluster;
    private final Bucket bucket;
    private final ObjectMapper mapper = new ObjectMapper();

    public CouchbaseResultsStore(String server) {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .connectTimeout(60000)
                .kvTimeout(60000)
                .build();

        cluster = CouchbaseCluster.create(env, server);
        bucket = cluster.openBucket(BucketName);
    }

    @Override
    public List<Query> getPendingResults() {
        return getResults(
                st -> st.getStatus() == SentimentTimeline.Status.Pending,
                SentimentTimeline::getQuery);
    }

    @Override
    public List<SentimentTimeline> getResults() {
        return getResults(
                st -> st.getStatus() != SentimentTimeline.Status.Pending,
                st -> st);
    }

    private <R> List<R> getResults(Predicate<SentimentTimeline> filter, Function<SentimentTimeline, R> resultMap) {
        List<ViewRow> rows = getAllRows();
        return rows.stream()
                .flatMap(row -> {
                    JsonDocument doc = row.document();
                    return doc == null ? Stream.empty() : Stream.of(doc.content().toString());
                })
                .flatMap(this::deserialize)
                .filter(filter)
                .sorted(Comparator.comparing(s -> s.getQuerySubmitTime()))
                .map(resultMap)
                .collect(Collectors.toList());
    }

    private List<ViewRow> getAllRows() {
        ViewResult result = bucket.query(ViewQuery.from(DesignName, ViewName));
        return result.allRows();
    }

    private Stream<SentimentTimeline> deserialize(String json) {
        try {
            return Stream.of(mapper.readValue(json, SentimentTimeline.class));
        } catch (IOException e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    @Override
    public void storePendingResults(Query query) {
        storeResults(SentimentTimeline.pending(query));
    }

    @Override
    public void storeResults(SentimentTimeline result) {
        try {
            JsonObject jsonObj = JsonObject.fromJson(mapper.writeValueAsString(result));
            JsonDocument doc = JsonDocument.create(result.getQueryId().toString(), jsonObj);
            bucket.upsert(doc);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        bucket.close();
        cluster.disconnect();
    }

    public void clear() {
        List<ViewRow> rows = getAllRows();

        rows.stream()
            .map(vr -> vr.id())
            .forEach(id -> {
                try {
                    bucket.remove(id);
                } catch (DocumentDoesNotExistException e) {
                    e.printStackTrace();
                }
            });
    }
}
