package integrationtest.zuhlke.ta.prototype;

import com.zuhlke.ta.prototype.Importer;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.SentimentTimeline;
import com.zuhlke.ta.prototype.inmemory.InMemoryTweetStore;
import com.zuhlke.ta.prototype.PersistentTweetService;
import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;
import org.junit.Test;

import java.io.File;

public class InMemoryTweetServiceIntegrationTest {
    @Test
    public void testAnalyzingTweets() throws Exception {
        PersistentTweetService tweetService = new PersistentTweetService(new TwitterSentimentAnalyzerImpl(), new InMemoryTweetStore());
        Importer importer = new Importer(tweetService);
        importer.importTweetsFrom(new File("test_set_tweets.txt"));
        SentimentTimeline timeline = tweetService.analyzeSentimentOverTime(new Query(""));
        System.out.println(timeline);
    }
}
