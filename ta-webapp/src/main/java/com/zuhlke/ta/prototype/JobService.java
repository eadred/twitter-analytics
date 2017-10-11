package com.zuhlke.ta.prototype;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class JobService extends Thread {
    private final ResultsStore resultsStore;
    private final BlockingQueue<Query> pendingQueries = new LinkedBlockingQueue<>();
    private TweetService tweetService;

    public JobService(TweetService tweetService, ResultsStore resultsStore) {
        this.tweetService = tweetService;
        this.resultsStore = resultsStore;
        this.start();
    }

    public void enqueueQuery(Query q) {
        try {
            pendingQueries.put(q);
        } catch (InterruptedException e) {
            // this is ok
        }
    }

    public List<SentimentTimeline> getResults() {
        return resultsStore.getResults();
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        try {
            while (true) {
                Optional.ofNullable(pendingQueries.poll(1, TimeUnit.SECONDS))
                        .map(q -> tweetService.analyzeSentimentOverTime(q))
                        .ifPresent(resultsStore::storeResults);
            }
        } catch (InterruptedException e) {
            // this is ok
        }
    }

    public List<Query> getPending() {
        return new ArrayList<>(pendingQueries);
    }
}
