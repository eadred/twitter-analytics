package com.zuhlke.ta.prototype.solutions.inmemory;

import com.zuhlke.ta.prototype.ResultsStore;
import com.zuhlke.ta.prototype.SentimentTimeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryResultsStore implements ResultsStore {
    private final List<SentimentTimeline> results = Collections.synchronizedList(new ArrayList<>());

    @Override
    public List<SentimentTimeline> getResults() {
        return results;
    }

    @Override
    public void storeResults(SentimentTimeline result) {
        results.add(result);
    }
}
