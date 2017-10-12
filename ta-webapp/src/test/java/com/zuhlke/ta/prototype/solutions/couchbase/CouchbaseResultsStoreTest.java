package com.zuhlke.ta.prototype.solutions.couchbase;

import com.zuhlke.ta.SlowTests;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.SentimentTimeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

public class CouchbaseResultsStoreTest {
    private static final String CouchbaseServer = "192.168.99.100";
    private CouchbaseResultsStore target;

    @Before
    public void before() {
        target = new CouchbaseResultsStore(CouchbaseServer);
    }

    @After
    public void after() {
        target.clear();
        target.close();
    }

    @Test
    @Category(SlowTests.class)
    public void retrieving_results_immediately_after_inserting_should_not_throw() {
        // Inserted documents aren't available to read immediately
        SentimentTimeline testResult = createTestResult();
        target.storeResults(testResult);
        target.getResults();
    }

    @Test
    @Category(SlowTests.class)
    public void inserted_results_should_be_retrievable() throws InterruptedException {
        SentimentTimeline testResult = createTestResult();

        target.storeResults(testResult);

        Optional<SentimentTimeline> result = tryGetResultFrom(() -> target.getResults())
                .thatMatches(testResult)
                .usingId(st -> st.getQueryId());

        assertTrue(result.isPresent());
    }

    @Test
    @Category(SlowTests.class)
    public void pending_results_should_be_retrievable() throws InterruptedException {
        Query testQuery = new Query("foo");

        target.storePendingResults(testQuery);

        Optional<Query> result = tryGetResultFrom(() -> target.getPendingResults())
                .thatMatches(testQuery)
                .usingId(q -> q.getId());

        assertTrue(result.isPresent());
    }

    @Test
    @Category(SlowTests.class)
    public void should_be_able_to_update_pending_result() throws InterruptedException {
        Query testQuery = new Query("foo");
        target.storePendingResults(testQuery);

        SentimentTimeline testResult = SentimentTimeline.completed(testQuery, new LinkedHashMap<>());
        target.storeResults(testResult);

        // Pending result should not be present after being upgraded
        Optional<Query> pendingResult = tryGetResultFrom(() -> target.getPendingResults())
                .thatMatches(testQuery)
                .usingId(q -> q.getId());

        assertFalse(pendingResult.isPresent());

        // Completed result should be present
        Optional<SentimentTimeline> result = tryGetResultFrom(() -> target.getResults())
                .thatMatches(testResult)
                .usingId(st -> st.getQueryId());

        assertTrue(result.isPresent());
    }

    private <T> Matcher<T> tryGetResultFrom(Supplier<List<T>> getAllResults) {
        return new Matcher<T>(getAllResults);
    }

    private SentimentTimeline createTestResult() {
        final Query Query = new Query("foo");
        final String Day1 = "2017-10-10";
        final String Day2 = "2017-10-11";

        Map<String, SentimentTimeline.Day> days = new LinkedHashMap<>();
        days.put(Day1, new SentimentTimeline.Day(5, 2));
        days.put(Day2, new SentimentTimeline.Day(3, 4));

        return SentimentTimeline.completed(Query, days);
    }

    private class Matcher<T> {
        private final Supplier<List<T>> getAllResults;
        private T expected;

        public Matcher(Supplier<List<T>> getAllResults) {
            this.getAllResults = getAllResults;
        }

        public Matcher<T> thatMatches(T expected) {
            this.expected = expected;
            return this;
        }

        public Optional<T> usingId(Function<T, UUID> getId) throws InterruptedException {
            Optional<T> result = Optional.empty();
            int count = 0;
            while (!result.isPresent() && count < 10) {
                count++;
                System.out.println(String.format("Waiting for results %d", count));
                Thread.sleep(1000);

                result = getAllResults.get()
                        .stream()
                        .filter(q -> getId.apply(q).equals(getId.apply(expected)))
                        .findFirst();
            }

            return result;
        }
    }
}
