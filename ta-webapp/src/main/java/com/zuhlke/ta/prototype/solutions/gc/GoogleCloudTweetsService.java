package com.zuhlke.ta.prototype.solutions.gc;

import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.SentimentTimeline;
import com.zuhlke.ta.prototype.TweetService;

/**
 * Created by eabi on 21/09/2017.
 */
public class GoogleCloudTweetsService implements TweetService {
    private final GoogleCloudSentimentTimelineAnalyzer sentimentTimelineAnalyzer;

    public GoogleCloudTweetsService(
            GoogleCloudSentimentTimelineAnalyzer sentimentTimelineAnalyzer) {
        this.sentimentTimelineAnalyzer = sentimentTimelineAnalyzer;
    }

    @Override
    public SentimentTimeline analyzeSentimentOverTime(Query q) {
        return sentimentTimelineAnalyzer.analyzeSentimentOverTime(q);
    }
}
