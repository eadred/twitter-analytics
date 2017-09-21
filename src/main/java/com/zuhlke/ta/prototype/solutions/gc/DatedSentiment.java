package com.zuhlke.ta.prototype.solutions.gc;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class DatedSentiment {
    private Double sentiment;
    private String date;

    public DatedSentiment(String date, Double sentiment) {
        this.date  = date;
        this.sentiment = sentiment;
    }

    public String getDate() { return date; }

    public Double getSentiment() { return sentiment; }
}
