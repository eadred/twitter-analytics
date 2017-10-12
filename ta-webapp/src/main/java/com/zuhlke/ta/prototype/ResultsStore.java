package com.zuhlke.ta.prototype;

import edu.stanford.nlp.pipeline.CoreNLPProtos;

import java.io.Closeable;
import java.util.List;;

public interface ResultsStore extends Closeable {
    List<Query> getPendingResults();

    List<SentimentTimeline> getResults();

    void storePendingResults(Query query);

    void storeResults(SentimentTimeline result);
}
