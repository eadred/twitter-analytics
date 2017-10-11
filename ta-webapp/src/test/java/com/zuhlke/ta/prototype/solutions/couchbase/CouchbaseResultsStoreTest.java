package com.zuhlke.ta.prototype.solutions.couchbase;

import com.zuhlke.ta.SlowTests;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.SentimentTimeline;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CouchbaseResultsStoreTest {
    private static final String CouchbaseServer = "192.168.99.100";
    @Test
    @Category(SlowTests.class)
    public void insertTest() {
        SentimentTimeline testResult = createTestResult();

        CouchbaseResultsStore target = new CouchbaseResultsStore(CouchbaseServer);
        target.storeResults(testResult);
    }

    @Test
    @Category(SlowTests.class)
    public void getResultsTest() {
        CouchbaseResultsStore target = new CouchbaseResultsStore(CouchbaseServer);

        List<SentimentTimeline> results = target.getResults();

        System.out.println("Done");
    }

    private SentimentTimeline createTestResult() {
        final Query Query = new Query("foo");
        final String Day1 = "2017-10-10";
        final String Day2 = "2017-10-11";

        Map<String, SentimentTimeline.Day> days = new LinkedHashMap<>();
        days.put(Day1, new SentimentTimeline.Day(5, 2));
        days.put(Day2, new SentimentTimeline.Day(3, 4));

        return new SentimentTimeline(Query, days);
    }
}
