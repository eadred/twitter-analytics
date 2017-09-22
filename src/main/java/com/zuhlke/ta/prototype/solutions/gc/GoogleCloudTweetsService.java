package com.zuhlke.ta.prototype.solutions.gc;

import com.google.cloud.bigquery.*;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.SentimentTimeline;
import com.zuhlke.ta.prototype.Tweet;
import com.zuhlke.ta.prototype.TweetService;

import java.io.IOException;
import java.util.*;

/**
 * Created by eabi on 21/09/2017.
 */
public class GoogleCloudTweetsService implements TweetService {
    private final DataFlowOptions options;
    private final SentimentDataFlowRunner runner;

    public GoogleCloudTweetsService(DataFlowOptions options) {
        this.options = options;
        this.runner = new SentimentDataFlowRunner(options);
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
        // Ignore
    }

    private Map<String, SentimentTimeline.Day> getResults() throws InterruptedException {
        Map<String, SentimentTimeline.Day> returnVal = new HashMap<>() ;

        // Instantiates a client
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

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
