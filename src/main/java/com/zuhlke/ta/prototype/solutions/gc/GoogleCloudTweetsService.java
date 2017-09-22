package com.zuhlke.ta.prototype.solutions.gc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.SentimentTimeline;
import com.zuhlke.ta.prototype.Tweet;
import com.zuhlke.ta.prototype.TweetService;
import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by eabi on 21/09/2017.
 */
public class GoogleCloudTweetsService implements TweetService {
    private final GoogleCloudSentimentTimelineAnalyzer sentimentTimelineAnalyzer;
    private final GoogleCloudTweetsImporter tweetsImporter;

    public GoogleCloudTweetsService(
            GoogleCloudSentimentTimelineAnalyzer sentimentTimelineAnalyzer,
            GoogleCloudTweetsImporter tweetsImporter) {
        this.sentimentTimelineAnalyzer = sentimentTimelineAnalyzer;
        this.tweetsImporter = tweetsImporter;
    }

    @Override
    public SentimentTimeline analyzeSentimentOverTime(Query q) {
        return sentimentTimelineAnalyzer.analyzeSentimentOverTime(q);
    }

    @Override
    public void importTweets(Collection<Tweet> tweets) {
        tweetsImporter.importTweets(tweets);
    }




}
