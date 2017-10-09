package com.zuhlke.ta.twitterclient;

import com.zuhlke.ta.prototype.Tweet;
import com.zuhlke.ta.prototype.solutions.gc.TweetsImporter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by eabi on 04/09/2017.
 */
public class TweetBuffer implements TweetSink {
    private final TweetsImporter tweetsImporter;
    private final int bufferSize;
    private final List<Tweet> buffer = new ArrayList<>();

    public TweetBuffer(TweetsImporter tweetsImporter, int bufferSize) {
        this.tweetsImporter = tweetsImporter;
        this.bufferSize = bufferSize;
    }

    @Override
    public void addTweet(Tweet tweet) {
        buffer.add(tweet);

        if (buffer.size() >= bufferSize) {
            System.out.println("Sending tweets...");
            tweetsImporter.importTweets(buffer);
            buffer.clear();
        }
    }
}
