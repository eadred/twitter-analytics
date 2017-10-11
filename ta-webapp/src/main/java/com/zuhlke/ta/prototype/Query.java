package com.zuhlke.ta.prototype;

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.Instant;

import java.util.UUID;

public class Query {
    public final String keyword;
    public final Instant submitTime;
    public final UUID id;

    public Query() { this(""); } // For Jackson

    public Query(String keyword) {
        this.keyword = keyword;
        this.submitTime = Instant.now();
        this.id = UUID.randomUUID();
    }

    @JsonProperty
    public String getKeyword() {
        return keyword;
    }

    @JsonProperty
    @JsonSerialize(using=InstantSerializer.class)
    @JsonDeserialize(using=InstantDeserializer.class)
    public Instant getSubmitTime() { return submitTime; }

    @JsonProperty
    public UUID getId() { return id; }

    @Override
    public String toString() {
        return "Query{" +
                "keyword='" + keyword + '\'' +
                '}';
    }
}
