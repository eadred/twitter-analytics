package com.zuhlke.ta.prototype.solutions.inmemory;

import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.ResultsStore;
import com.zuhlke.ta.prototype.SentimentTimeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryResultsStore implements ResultsStore {
    private final List<Query> pendingResults = Collections.synchronizedList(new ArrayList<>());
    private final List<SentimentTimeline> results = Collections.synchronizedList(new ArrayList<>());

    @Override
    public List<Query> getPendingResults() { return pendingResults; }

    @Override
    public List<SentimentTimeline> getResults() {
        return results;
    }

    @Override
    public void storePendingResults(Query query) {
        pendingResults.add(query);
    }

    @Override
    public void storeResults(SentimentTimeline result) {
        results.add(result);
        pendingResults.removeIf(q -> q.getId() == result.getQueryId());
    }
}
