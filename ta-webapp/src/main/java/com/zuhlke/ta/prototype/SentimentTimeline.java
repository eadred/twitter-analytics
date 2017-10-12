package com.zuhlke.ta.prototype;

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonIgnore;
import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Instant;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class SentimentTimeline {
    private final Query query;
    private final Map<String, Day> days;
    private final Status status;

    public SentimentTimeline() { this(new Query(), new LinkedHashMap<>(), Status.Pending); } // For Jackson

    public static SentimentTimeline pending(Query query) {
        return new SentimentTimeline(query, new LinkedHashMap<>(), Status.Pending);
    }

    public static SentimentTimeline failed(Query query) {
        return new SentimentTimeline(query, new LinkedHashMap<>(), Status.Failed);
    }

    public static SentimentTimeline completed(Query query, Map<String, Day> days) {
        return new SentimentTimeline(query, days, Status.Completed);
    }

    private SentimentTimeline(Query query, Map<String, Day> days, Status status) {
        this.query = query;
        this.days = days;
        this.status = status;
    }

    public static class Day {
        private int goodTweets = 0;
        private int badTweets = 0;

        public Day() {
            this(0, 0);
        }

        public Day(int goodTweets, int badTweets) {
            this.goodTweets = goodTweets;
            this.badTweets = badTweets;
        }

        public static Day merge(Day left, Day right) {
            return new Day(left.goodTweets + right.goodTweets, left.badTweets + right.badTweets);
        }

        @JsonProperty
        public int getGoodTweets() { return goodTweets; }

        @JsonProperty
        public int getBadTweets() { return badTweets; }

        public void incrementGood() { goodTweets++; }

        public void incrementBad() { badTweets++; }

        @Override
        public String toString() {
            return format("{goodTweets=%d, badTweets=%d}", goodTweets, badTweets);
        }
    }

    @JsonProperty
    public Query getQuery() {
        return query;
    }

    @JsonProperty
    public Map<String, Day> getDays() {
        return days;
    }

    @JsonProperty
    public Status getStatus() { return status; }

    @JsonIgnore
    public Instant getQuerySubmitTime() { return getQuery().getSubmitTime(); }

    @JsonIgnore
    public UUID getQueryId() { return getQuery().getId(); }

    @Override
    public String toString() {
        return query + "\n" +
                days.entrySet().stream()
                        .map(e -> format("%s\t%s", e.getKey(), e.getValue()))
                        .collect(joining("\n"));
    }

    public enum Status {
        Pending,
        Failed,
        Completed
    }
}
