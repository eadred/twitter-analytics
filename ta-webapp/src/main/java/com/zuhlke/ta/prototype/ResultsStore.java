package com.zuhlke.ta.prototype;

import edu.stanford.nlp.pipeline.CoreNLPProtos;

import java.util.List;;

public interface ResultsStore {
    List<Query> getPendingResults();

    List<SentimentTimeline> getResults();

    void storePendingResults(Query query);

    void storeResults(SentimentTimeline result);
}
