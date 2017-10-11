package com.zuhlke.ta.prototype;

import edu.stanford.nlp.pipeline.CoreNLPProtos;

import java.util.List;;

public interface ResultsStore {
    List<SentimentTimeline> getResults();

    void storeResults(SentimentTimeline result);
}
