package com.zuhlke.ta.prototype.solutions.gc;

import com.google.cloud.bigquery.*;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.prototype.SentimentTimeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GoogleCloudSentimentTimelineAnalyzer {
    private final BigQuery bigquery;
    private final String dataset;
    private final String sourceTable;

    public GoogleCloudSentimentTimelineAnalyzer(
            BigQuery bigquery,
            ApplicationOptions options) {
        this.bigquery = bigquery;
        this.dataset = options.dataset;
        this.sourceTable = options.sourceTable;
    }

    public SentimentTimeline analyzeSentimentOverTime(Query q) {
        try {
            return getResults(q);
        } catch (InterruptedException e) {
            System.out.println(e.toString());
            return new SentimentTimeline(q.getKeyword());
        } catch (Exception e) {
            System.out.println(e.toString());
            return new SentimentTimeline(q.getKeyword());
        }
    }

    private SentimentTimeline getResults(Query q) throws InterruptedException {
        QueryResponse positiveResp = runQuery(q, SentimentType.Positive);
        QueryResponse negativeResp = runQuery(q, SentimentType.Negative);

        Map<String, SentimentTimeline.Day> positiveResults = getResultsFromResponse(positiveResp, SentimentType.Positive);
        Map<String, SentimentTimeline.Day> negativeResults = getResultsFromResponse(negativeResp, SentimentType.Negative);

        Map<String, SentimentTimeline.Day> merged = mergeResults(positiveResults, negativeResults);

        return new SentimentTimeline(q.getKeyword(), merged);
    }

    private QueryResponse runQuery(Query q, SentimentType type) {
        String query = String.format(
                "SELECT date, COUNT(date) as count FROM %s.%s WHERE sentiment %s 0.0 AND STRPOS(UPPER(message), UPPER(@keyword)) <> 0 GROUP BY date",
                dataset,
                sourceTable,
                type == SentimentType.Negative ? "<" : ">");

        QueryRequest request = QueryRequest.newBuilder(query)
                .addNamedParameter("keyword", QueryParameterValue.string(q.getKeyword()))
                .setUseLegacySql(false)
                .build();

        return bigquery.query(request);
    }

    private Map<String, SentimentTimeline.Day> getResultsFromResponse(QueryResponse response, SentimentType type) throws InterruptedException {
        Map<String, SentimentTimeline.Day> returnVal = new HashMap<>() ;

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
            int count = (int)row.get(1).getLongValue();

            SentimentTimeline.Day day = type == SentimentType.Negative ? new SentimentTimeline.Day(0, count) : new SentimentTimeline.Day(count, 0);
            returnVal.put(date, day);
        }

        return returnVal;
    }

    private static Map<String, SentimentTimeline.Day> mergeResults(Map<String, SentimentTimeline.Day> positive, Map<String, SentimentTimeline.Day> negative) {
        return Stream.of(positive, negative)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, SentimentTimeline.Day::merge));
    }

    private enum SentimentType {
        Negative,
        Positive
    }
}
