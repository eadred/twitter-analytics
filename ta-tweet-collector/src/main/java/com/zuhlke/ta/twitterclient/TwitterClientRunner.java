package com.zuhlke.ta.twitterclient;

import com.zuhlke.ta.prototype.solutions.gc.TweetsImporter;
import twitter4j.StatusListener;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by eabi on 04/09/2017.
 */
public class TwitterClientRunner {
    public static void runClient(TweetsImporter tweetsImporter) throws IOException {
        Properties props = new Properties();
        props.load(TwitterClientRunner.class.getClassLoader().getResourceAsStream("tweet-collector.properties"));

        LocationBounds bounds = new LocationBounds(
                Double.parseDouble(props.getProperty("boundsLatitudeMin")),
                Double.parseDouble(props.getProperty("boundsLongitudeMin")),
                Double.parseDouble(props.getProperty("boundsLatitudeMax")),
                Double.parseDouble(props.getProperty("boundsLongitudeMax")));

        StatusListener listener = new Listener(new TweetBuffer(
                tweetsImporter,
                Integer.parseInt(props.getProperty("tweetsBufferSize"))));

        TwitterClient client = new TwitterClient(
                listener,
                bounds);

        client.run();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> client.close()));
    }
}
