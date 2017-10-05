package com.zuhlke.ta.prototype.solutions.gc;

import com.zuhlke.ta.prototype.Tweet;

import java.util.Collection;

public interface GoogleCloudTweetsImporter {
    void importTweets(Collection<Tweet> tweets);
}
