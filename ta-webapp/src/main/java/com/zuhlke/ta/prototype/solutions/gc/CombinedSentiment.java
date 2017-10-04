package com.zuhlke.ta.prototype.solutions.gc;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class CombinedSentiment {
    private int numPositive;
    private int numNegative;

    public CombinedSentiment() {
        this.numPositive = 0;
        this.numNegative = 0;
    }

    public CombinedSentiment(int numPositive, int numNegative) {
        this.numPositive = numPositive;
        this.numNegative = numNegative;
    }

    public int getNumPositive() { return numPositive; }

    public int getNumNegative() { return numNegative; }
}
