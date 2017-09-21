package com.zuhlke.ta.prototype.solutions.gc;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class DatedMessage {
    private String message;
    private String date;

    public DatedMessage(String date, String message) {
        this.date  = date;
        this.message = message;

    }

    public String getDate() { return date; }

    public String getMessage() { return message; }
}
