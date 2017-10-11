package com.zuhlke.ta.prototype;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class SentimentTimelineTest {
    @Test
    public void roundTripSerialization() throws JsonProcessingException, IOException {
        final String Query = "foo";
        final String Day1 = "2017-10-10";
        final String Day2 = "2017-10-11";

        Map<String, SentimentTimeline.Day> days = new LinkedHashMap<>();
        days.put(Day1, new SentimentTimeline.Day(5, 2));
        days.put(Day2, new SentimentTimeline.Day(3, 4));

        SentimentTimeline target = new SentimentTimeline(Query, days);

        ObjectMapper mapper = new ObjectMapper();

        String serialized = mapper.writeValueAsString(target);

        SentimentTimeline deserialized = mapper.readValue(serialized, SentimentTimeline.class);

        assertEquals(target.getQuery(), deserialized.getQuery());
        assertEquals(target.getDays().size(), deserialized.getDays().size());

        assertDaysEqual(target.getDays().get(Day1), deserialized.getDays().get(Day1));
        assertDaysEqual(target.getDays().get(Day2), deserialized.getDays().get(Day2));
    }

    private void assertDaysEqual(SentimentTimeline.Day expected, SentimentTimeline.Day actual) {
        assertEquals(expected.getGoodTweets(), actual.getGoodTweets());
        assertEquals(expected.getBadTweets(), actual.getBadTweets());
    }
}
