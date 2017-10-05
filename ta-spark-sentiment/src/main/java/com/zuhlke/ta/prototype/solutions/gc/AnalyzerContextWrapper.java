package com.zuhlke.ta.prototype.solutions.gc;


import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Interval;

import java.util.UUID;

public class AnalyzerContextWrapper {
    private static final Duration MAX_UNUSED_TIME = Duration.standardMinutes(5);
    private AnalyzerContext context;
    private State state;
    private Instant lastUsedTime;

    public AnalyzerContextWrapper(UUID id) {
        context = new AnalyzerContext(id);
        state = State.New;
        updateUsedTime();
    }

    public AnalyzerContext getContext() { return context; }


    public boolean isAvailable() {
        return state == State.Available;
    }

    public void setInUse() {
        state = State.InUse;
        updateUsedTime();
    }

    public void setAvailable() {
        state = State.Available;
        updateUsedTime();
    }

    public boolean isOld() {
        Duration sinceLastUsed = new Interval(lastUsedTime, Instant.now()).toDuration();
        return sinceLastUsed.compareTo(MAX_UNUSED_TIME) > 0;
    }

    private void updateUsedTime() {
        this.lastUsedTime = Instant.now();
    }

    public enum State {
        New,
        InUse,
        Available
    }
}
