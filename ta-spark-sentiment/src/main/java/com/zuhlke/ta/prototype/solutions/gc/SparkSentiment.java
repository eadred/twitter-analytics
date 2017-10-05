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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;
import org.spark_project.guava.collect.Iterators;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import java.util.stream.Collectors;

public class SparkSentiment {

    private static final String ID_COL = "id";
    private static final String USER_ID_COL = "userid";
    private static final String MESSAGE_COL = "message";
    private static final String DATE_COL = "date";
    private static final String SENTIMENT_COL = "sentiment";

    public static final int DEFAULT_WINDOW_SIZE_SECS = 60;
    public static final int DEFAULT_PARTITIONS = 4;
    public static final String ENGLISH = "en";

    private static String projectId;
    private static String topicName;
    private static String datasetId;
    private static String tableId;
    private static int windowSizeSecs;

    public static void main(String[] args) {
        parseArgs(args);

        Logger logger = LogManager.getRootLogger();

        logger.info("Setting up Spark sentiment analysis");

        SparkGCPCredentials cred = new SparkGCPCredentials.Builder().metadataServiceAccount().build();

        SparkConf conf = new SparkConf().setAppName("Sentiment");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<SparkPubsubMessage> receiver = PubsubUtils.createStream(
                ssc,
                projectId,
                topicName,
                "SentimentSubscription",
                cred,
                StorageLevel.MEMORY_AND_DISK());

        // Create local variables from static members for use in the sendTweetsToBigQuery closure
        String dataset = datasetId;
        String table = tableId;
        logger.info(String.format("Output table will be %s.%s", dataset, table));

        receiver
                .flatMap(SparkSentiment::getTweetFromMsg)
                .filter(t -> t.lang.equalsIgnoreCase(ENGLISH))
                .window(Durations.seconds(windowSizeSecs), Durations.seconds(windowSizeSecs))
                .mapPartitions(SparkSentiment::calcSentiment)
                .mapPartitions(ts -> sendTweetsToBigQuery(ts, dataset, table))
                .print();

        logger.info("Starting Spark sentiment analysis");

        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
        }

        logger.info("Completed Spark sentiment analysis");
    }

    private static void parseArgs(String[] args) {
        projectId = args[0];
        topicName = args[1];
        datasetId = args[2];
        tableId = args[3];

        windowSizeSecs = DEFAULT_WINDOW_SIZE_SECS;
        if (args.length > 4) {
            try {
                windowSizeSecs = Integer.parseInt(args[4]);
            } catch (NumberFormatException e) {
            }
        }
    }

    private static Iterator<Tweet> getTweetFromMsg(SparkPubsubMessage msg) {
        Logger logger = LogManager.getRootLogger();

        ObjectMapper mapper = new ObjectMapper();
        String payload;
        try {
            payload = new String(msg.getData(), "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            logger.error("Error getting PubSub payload", e);
            return Iterators.emptyIterator();
        }

        try {
            return Iterators.singletonIterator(mapper.readValue(payload, Tweet.class));
        }
        catch (IOException e) {
            logger.error("Error deserializing PubSub payload", e);
            return Iterators.emptyIterator();
        }
    }

    private static Iterator<Tweet> calcSentiment(Iterator<Tweet> tweets) {
        Logger logger = LogManager.getRootLogger();
        logger.info("Calculating sentiment");

        if (!tweets.hasNext()) return Iterators.emptyIterator();

        TwitterSentimentAnalyzerImpl analyzer = new TwitterSentimentAnalyzerImpl();

        return Streams.stream(tweets)
                .map(t -> new Tweet(t.id, t.userId, t.message, t.getDate(), t.lang, analyzer.getSentiment(t.message)))
                .collect(Collectors.toList())
                .iterator();
    }

    private static Iterator<String> sendTweetsToBigQuery(
            Iterator<Tweet> tweets,
            String datasetId,
            String tableId) {

        if (!tweets.hasNext()) return Iterators.emptyIterator();

        BigQuery query = BigQueryOptions.getDefaultInstance().getService();

        List<InsertAllRequest.RowToInsert> rows = Streams.stream(tweets)
                .map(SparkSentiment::tweetToRow)
                .collect(Collectors.toList());

        InsertAllResponse resp = query.insertAll(InsertAllRequest.newBuilder(
                datasetId,
                tableId,
                rows)
                .build());

        Logger logger = LogManager.getRootLogger();
        if (resp.hasErrors()) {
            logger.error("Error inserting into BiqQuery");

            return resp.getInsertErrors()
                    .values()
                    .stream()
                    .flatMap(errs -> errs.stream())
                    .limit(10)
                    .map(bqerr -> bqerr.getMessage())
                    .iterator();
        } else {
            logger.info("Inserted rows into BiqQuery");
            return Iterators.singletonIterator(String.format("Inserted %d rows", rows.size()));
        }
    }

    private static InsertAllRequest.RowToInsert tweetToRow(Tweet tweet) {
        Map<String, Object> map = new HashMap<>();
        map.put(ID_COL, tweet.id);
        map.put(USER_ID_COL, tweet.userId);
        map.put(MESSAGE_COL, tweet.message);
        map.put(DATE_COL, tweet.date);
        map.put(SENTIMENT_COL, tweet.sentiment);
        return InsertAllRequest.RowToInsert.of(map);
    }
}
