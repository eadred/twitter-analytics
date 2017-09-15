package com.zuhlke.ta.prototype;

import com.zuhlke.ta.common.Query;
import com.zuhlke.ta.common.SentimentTimeline;
import com.zuhlke.ta.common.Tweet;
import com.zuhlke.ta.common.TweetService;
import com.zuhlke.ta.prototype.Importer;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class ImporterTest {
    @Test
    public void tryImporting() throws Exception {
        Importer importer = new Importer(new TweetService() {
            public SentimentTimeline analyzeSentimentOverTime(Query q) {
                return new SentimentTimeline(q.keyword);
            }

            public void importTweets(Collection<Tweet> tweets) {
                final AtomicLong count = new AtomicLong();
                tweets.stream().peek(t -> count.incrementAndGet() )
                       .forEach(System.out::println);
                System.out.println("imported " + count.longValue() + " tweets.");
            }
        });
        importer.importTweetsFrom(new File("minimal_set_tweets.txt"));
    }
}
