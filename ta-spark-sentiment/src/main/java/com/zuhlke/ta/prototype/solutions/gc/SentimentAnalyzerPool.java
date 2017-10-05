package com.zuhlke.ta.prototype.solutions.gc;


import java.util.*;
import java.util.stream.Collectors;

public final class SentimentAnalyzerPool {

    public static SentimentAnalyzerPool Instance;

    public final Map<UUID, AnalyzerContextWrapper> analyzers;

    static {
        new SentimentAnalyzerPool();
    }

    private SentimentAnalyzerPool() {
        analyzers = new HashMap<>();
        Instance = this;
    }

    public synchronized AnalyzerContext getAnalyzer() {
        AnalyzerContextWrapper wrapper;

        Optional<AnalyzerContextWrapper> maybeFirstAvailable =  analyzers.values().stream()
                .filter(c -> c.isAvailable())
                .findFirst();

        if (maybeFirstAvailable.isPresent()) {
            wrapper = maybeFirstAvailable.get();
        } else {
            UUID id = UUID.randomUUID();
            wrapper = new AnalyzerContextWrapper(id);
            analyzers.put(id, wrapper);
        }

        wrapper.setInUse();

        return wrapper.getContext();
    }

    public synchronized void returnAnalyzer(AnalyzerContext analyzer) {
        if (!analyzers.containsKey(analyzer.getId())) return;

        AnalyzerContextWrapper wrapper = analyzers.get(analyzer.getId());

        wrapper.setAvailable();

        // Remove anything that hasn't been used in a while
        List<UUID> idsToRemove = analyzers.values().stream()
                .filter(c -> c.isOld())
                .map(c -> c.getContext().getId())
                .collect(Collectors.toList());

        idsToRemove.forEach(id -> analyzers.remove(id));
    }
}
