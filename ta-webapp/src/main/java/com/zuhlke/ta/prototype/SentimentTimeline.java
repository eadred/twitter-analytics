package com.zuhlke.ta.prototype;

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class SentimentTimeline {
    private final String query;
    private final Map<String, Day> days;

    public SentimentTimeline() { this(""); } // For Jackson

    public SentimentTimeline(String query) {
        this(query, new LinkedHashMap<>());
    }

    public SentimentTimeline(String query, Map<String, Day> days) {
        this.query = query;
        this.days = days;
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
    public String getQuery() {
        return query;
    }

    @JsonProperty
    public Map<String, Day> getDays() {
        return days;
    }

    @Override
    public String toString() {
        return query + "\n" +
                days.entrySet().stream()
                        .map(e -> format("%s\t%s", e.getKey(), e.getValue()))
                        .collect(joining("\n"));
    }

}
