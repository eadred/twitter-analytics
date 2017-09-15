package com.zuhlke.ta.common;

import java.util.Collection;

public interface TweetService {
    SentimentTimeline analyzeSentimentOverTime(Query q);
    void importTweets(Collection<Tweet> tweets);
}
