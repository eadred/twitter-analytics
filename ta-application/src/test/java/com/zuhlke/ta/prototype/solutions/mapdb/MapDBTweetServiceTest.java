package com.zuhlke.ta.prototype.solutions.mapdb;

import com.zuhlke.ta.SlowTests;
import com.zuhlke.ta.prototype.Importer;
import com.zuhlke.ta.common.Query;
import com.zuhlke.ta.common.SentimentTimeline;
import com.zuhlke.ta.prototype.solutions.mapdb.MapDBTweetService;
import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

import static com.zuhlke.ta.prototype.solutions.mapdb.MapDBTweetStore.TWEETS_DB;

public class MapDBTweetServiceTest {
    private MapDBTweetService tweetService;

    @Before
    public void setUp() throws Exception {
        new File(TWEETS_DB).delete();
        tweetService = new MapDBTweetService(new TwitterSentimentAnalyzerImpl());
    }

    @Test
    @Category(SlowTests.class)
    public void importingAndAnalyzeTweets() throws Exception {
        Importer importer = new Importer(tweetService);
        importer.importTweetsFrom(new File("test_set_tweets.txt"));
        SentimentTimeline timeline = tweetService.analyzeSentimentOverTime(new Query("Buhari"));
        System.out.println(timeline);
    }
}
