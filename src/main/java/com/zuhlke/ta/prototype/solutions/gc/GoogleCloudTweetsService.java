package com.zuhlke.ta.prototype.solutions.gc;

import com.google.cloud.bigquery.*;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.SentimentTimeline;
import com.zuhlke.ta.prototype.Tweet;
import com.zuhlke.ta.prototype.TweetService;
import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by eabi on 21/09/2017.
 */
public class GoogleCloudTweetsService implements TweetService {
    private final DataFlowOptions options;
    private final SentimentDataFlowRunner runner;
    private final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private final TwitterSentimentAnalyzerImpl sentimentAnalyzer;

    public GoogleCloudTweetsService(DataFlowOptions options) {
        this.options = options;
        this.runner = new SentimentDataFlowRunner(options);
        this.sentimentAnalyzer = new TwitterSentimentAnalyzerImpl();
    }

    @Override
    public SentimentTimeline analyzeSentimentOverTime(Query q) {
        runner.run(q);

        try {
            Map<String, SentimentTimeline.Day> results = getResults();
            return new SentimentTimeline(q.getKeyword(), results);
        } catch (InterruptedException e) {
            System.out.println(e.toString());
            return new SentimentTimeline(q.getKeyword());
        } catch (Exception e) {
            System.out.println(e.toString());
            return new SentimentTimeline(q.getKeyword());
        }
    }

    @Override
    public void importTweets(Collection<Tweet> tweets) {
        List<InsertAllRequest.RowToInsert> rows = tweets
                .stream()
                .map(this::tweetToInsertRow)
                .collect(Collectors.toList());

        InsertAllRequest req = InsertAllRequest.of(options.dataset, options.sourceTable, rows);
        InsertAllResponse resp = bigquery.insertAll(req);

        if (resp.hasErrors()) {
            System.err.println("Inserting tweets errored");
        } else {
            System.out.println("Inserted tweets");
        }
    }

    private InsertAllRequest.RowToInsert tweetToInsertRow(Tweet tweet) {
        Map<String, Object> values = new HashMap<>();
        values.put("id", tweet.id);
        values.put("userId", tweet.userId);
        values.put("message", tweet.message);
        values.put("date", tweet.date.toString());
        values.put("sentiment", sentimentAnalyzer.getSentiment(tweet.message));

        return InsertAllRequest.RowToInsert.of(values);
    }

    private Map<String, SentimentTimeline.Day> getResults() throws InterruptedException {
        Map<String, SentimentTimeline.Day> returnVal = new HashMap<>() ;

        String query = String.format(
                "SELECT %s, %s, %s FROM %s.%s ORDER BY date",
                SentimentDataFlowRunner.ResultsDateColumn,
                SentimentDataFlowRunner.ResultsPositiveColumn,
                SentimentDataFlowRunner.ResultsNegativeColumn,
                options.dataset,
                SentimentDataFlowRunner.ResultsTable);

        QueryRequest request = QueryRequest.of(query);
        QueryResponse response = bigquery.query(request);
        // Wait for things to finish
        while (!response.jobCompleted()) {
            Thread.sleep(1000);
            response = bigquery.getQueryResults(response.getJobId());
        }
        if (response.hasErrors()) {
            // handle errors
        }
        QueryResult result = response.getResult();

        for (List<FieldValue> row : result.iterateAll()) {
            String date = row.get(0).getStringValue();
            int positive = (int)row.get(1).getLongValue();
            int negative = (int)row.get(2).getLongValue();

            returnVal.put(date, new SentimentTimeline.Day(positive, negative));
        }

        return returnVal;
    }
}
