package com.zuhlke.ta.prototype.solutions.gc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.zuhlke.ta.prototype.Tweet;

import java.io.IOException;
import java.util.Collection;

public class GoogleCloudTweetsImporterImpl implements TweetsImporter {
    private final ObjectMapper mapper;
    private final Publisher publisher;

    public GoogleCloudTweetsImporterImpl(String projectId, String topicName) throws IOException {
        mapper = new ObjectMapper();

        publisher = Publisher.defaultBuilder(TopicName.create(projectId, topicName)).build();
    }

    @Override
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
