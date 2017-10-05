package com.zuhlke.ta.prototype.solutions.gc;

import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;

import java.util.UUID;

public class AnalyzerContext {
    private final UUID id;
    private TwitterSentimentAnalyzerImpl analyzer;

    public AnalyzerContext(UUID id) { this.id = id; }

    public UUID getId() {
        return id;
    }

    public TwitterSentimentAnalyzerImpl getAnalyzer() {
        if (analyzer == null) analyzer = new TwitterSentimentAnalyzerImpl();
        return analyzer;
    }
}
