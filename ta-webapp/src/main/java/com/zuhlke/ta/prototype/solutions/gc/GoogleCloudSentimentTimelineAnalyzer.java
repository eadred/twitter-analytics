package com.zuhlke.ta.prototype.solutions.gc;

import com.google.cloud.bigquery.*;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.SentimentTimeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoogleCloudSentimentTimelineAnalyzer {
    private final BigQuery bigquery;
    private final String resultsDataset;
    private final AggregationDataFlowRunner queryRunner;

    public GoogleCloudSentimentTimelineAnalyzer(
            BigQuery bigquery,
            ApplicationOptions options) {
        this.bigquery = bigquery;
        this.resultsDataset = options.dataset;
        this.queryRunner = new AggregationDataFlowRunner(options);
    }

    public SentimentTimeline analyzeSentimentOverTime(Query q) {
        queryRunner.run(q);

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

    private Map<String, SentimentTimeline.Day> getResults() throws InterruptedException {
        Map<String, SentimentTimeline.Day> returnVal = new HashMap<>() ;

        String query = String.format(
                "SELECT %s, %s, %s FROM %s.%s ORDER BY date",
                AggregationDataFlowRunner.ResultsDateColumn,
                AggregationDataFlowRunner.ResultsPositiveColumn,
                AggregationDataFlowRunner.ResultsNegativeColumn,
                resultsDataset,
                AggregationDataFlowRunner.ResultsTable);

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
