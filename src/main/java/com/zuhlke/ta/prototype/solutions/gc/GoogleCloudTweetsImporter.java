package com.zuhlke.ta.prototype.solutions.gc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import com.google.pubsub.v1.TopicName;
import com.zuhlke.ta.prototype.Tweet;
import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by eabi on 29/09/2017.
 */
public class GoogleCloudTweetsImporter {
    private final ObjectMapper mapper;
    private final Publisher publisher;

    private final SentimentDataFlowRunner sentimentRunner;

    public GoogleCloudTweetsImporter(ApplicationOptions options) throws IOException {
        mapper = new ObjectMapper();

        TopicName topicName = TopicName.create(options.projectId, options.topicName);
        publisher = Publisher.defaultBuilder(topicName).build();

        sentimentRunner = new SentimentDataFlowRunner(options);
        sentimentRunner.run();
    }

    public void importTweets(Collection<Tweet> tweets) {
        tweets
                .stream()
                .forEach(this::sendTweetToPubSub);
    }

    private void sendTweetToPubSub(Tweet tweet) {
        String tweetJson;
        try {
            tweetJson = mapper.writeValueAsString(tweet);
        } catch (JsonProcessingException e) {
            System.err.println(e);
            return;
        }

        ByteString data = ByteString.copyFromUtf8(tweetJson);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        publisher.publish(pubsubMessage);
    }
}
