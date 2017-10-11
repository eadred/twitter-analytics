package com.zuhlke.ta.prototype.solutions.couchbase;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.zuhlke.ta.prototype.ResultsStore;
import com.zuhlke.ta.prototype.SentimentTimeline;
import com.couchbase.client.java.*;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.*;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
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
                .connectTimeout(10000)
                .kvTimeout(10000)
                .build();

        cluster = CouchbaseCluster.create(env, server);
        bucket = cluster.openBucket(BucketName);
    }

    @Override
    public List<SentimentTimeline> getResults() {
        ViewResult result = bucket.query(ViewQuery.from(DesignName, ViewName));

        List<ViewRow> rows = result.allRows();
        return rows.stream()
                .map(row -> row.document().content().toString())
                .flatMap(this::deserialize)
                .sorted(Comparator.comparing(s -> s.getQuerySubmitTime()))
                .collect(Collectors.toList());
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
    public void storeResults(SentimentTimeline result) {
        try {
            JsonObject jsonObj = JsonObject.fromJson(mapper.writeValueAsString(result));
            JsonDocument doc = JsonDocument.create(result.getQueryId().toString(), jsonObj);
            bucket.upsert(doc);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
