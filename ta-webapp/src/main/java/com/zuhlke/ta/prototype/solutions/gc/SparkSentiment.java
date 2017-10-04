package com.zuhlke.ta.prototype.solutions.gc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.common.collect.Streams;
import com.zuhlke.ta.prototype.Tweet;
import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;
import org.apache.spark.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;

public class SparkSentiment {

    private static final String ID_COL = "id";
    private static final String USER_ID_COL = "userid";
    private static final String MESSAGE_COL = "message";
    private static final String DATE_COL = "date";
    private static final String SENTIMENT_COL = "sentiment";

    public static void main(String[] args) throws InterruptedException {
        String projectId = args[0];
        String topicName = args[1];
        String datasetId = args[2];
        String tableId = args[3];

        String topicNameFull = "projects/" + projectId + "/topics/" + topicName;

        SparkGCPCredentials cred = new SparkGCPCredentials.Builder().metadataServiceAccount().build();

        SparkConf conf = new SparkConf().setAppName("Sentiment");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<SparkPubsubMessage> receiver = PubsubUtils.createStream(
                ssc,
                projectId,
                topicNameFull,
                "SentimentSubscription",
                cred,
                StorageLevel.apply(true, true, false, 0));

        receiver
                .map(SparkSentiment::getTweetFromMsg)
                .filter(tw -> tw != null)
                .window(Durations.seconds(60))
                .mapPartitions(SparkSentiment::calcSentiment)
                .foreachRDD(rdd -> rdd.foreachPartition(ts -> sendTweetsToBigQuery(ts, datasetId, tableId)));

        ssc.start();
        ssc.awaitTermination();
    }

    private static Tweet getTweetFromMsg(SparkPubsubMessage msg) {
        ObjectMapper mapper = new ObjectMapper();
        String payload;
        try {
            payload = new String(msg.getData(), "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            return null;
        }

        try {
            return mapper.readValue(payload, Tweet.class);
        }
        catch (IOException e) {
            return null;
        }
    }

    private static Iterator<Tweet> calcSentiment(Iterator<Tweet> tweets) {
        TwitterSentimentAnalyzerImpl analyzer = new TwitterSentimentAnalyzerImpl();

        return Streams.stream(tweets)
                .map(t -> new Tweet(t.id, t.userId, t.message, t.getDate(), analyzer.getSentiment(t.message)))
                .collect(Collectors.toList())
                .iterator();
    }

    private static void sendTweetsToBigQuery(
            Iterator<Tweet> tweets,
            String datasetId,
            String tableId) {
        BigQuery query = BigQueryOptions.getDefaultInstance().getService();

        Iterable<InsertAllRequest.RowToInsert> rows = Streams.stream(tweets)
                .map(SparkSentiment::tweetToRow)
                .collect(Collectors.toList());

        InsertAllResponse resp = query.insertAll(InsertAllRequest.newBuilder(
                datasetId,
                tableId,
                rows)
                .build());
    }

    private static InsertAllRequest.RowToInsert tweetToRow(Tweet tweet) {
        Map<String, Object> map = new HashMap<>();
        map.put(ID_COL, tweet.id);
        map.put(USER_ID_COL, tweet.userId);
        map.put(MESSAGE_COL, tweet.message);
        map.put(DATE_COL, tweet.getDate());
        map.put(SENTIMENT_COL, tweet.sentiment);
        return InsertAllRequest.RowToInsert.of(map);
    }
}
