package com.zuhlke.ta.web;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.base.Strings;
import com.zuhlke.ta.prototype.*;
import com.zuhlke.ta.prototype.solutions.gc.ApplicationOptions;
import com.zuhlke.ta.prototype.solutions.gc.GoogleCloudSentimentTimelineAnalyzer;
import com.zuhlke.ta.prototype.solutions.gc.GoogleCloudTweetsImporter;
import com.zuhlke.ta.prototype.solutions.gc.GoogleCloudTweetsService;
import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;
import com.zuhlke.ta.twitterclient.TwitterClientRunner;
import org.joda.time.Instant;
import org.joda.time.Interval;
import spark.ModelAndView;
import spark.Request;
import spark.Response;
import spark.template.freemarker.FreeMarkerEngine;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

import static spark.Spark.get;
import static spark.Spark.post;

public class Application {
    public static void main(String[] args) throws IOException, URISyntaxException {
        ApplicationOptions options = ApplicationOptions.fromConfig();
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        GoogleCloudSentimentTimelineAnalyzer sentimentTimelineAnalyzer = new GoogleCloudSentimentTimelineAnalyzer(bigquery, options);
        GoogleCloudTweetsImporter tweetsImporter = new GoogleCloudTweetsImporter(options);
        TweetService tweetService = new GoogleCloudTweetsService(sentimentTimelineAnalyzer, tweetsImporter);
        JobService jobService = new JobService(tweetService);

        FreeMarkerEngine freeMarker = new FreeMarkerEngine();

        get("/", (req, resp) -> homepageData(jobService), freeMarker);
        get("/results/", (req, resp) -> jobService.getResults());
        get("/pending/", (req, resp) -> jobService.getPending());
        post("/jobs/", (req, resp) -> enqueueJob(jobService, req, resp));

        TwitterClientRunner.runClient(tweetService);
    }

    private static ModelAndView homepageData(JobService jobService) {
        Map<String, Object> model = new HashMap<>();
        model.put("results", jobService.getResults());
        model.put("pending", jobService.getPending());
        return new ModelAndView(model, "index.html");
    }

    private static Object enqueueJob(JobService jobService, Request req, Response resp) {
        String keyword = req.queryMap("keyword").value();
        Query q = new Query(keyword);
        if (!Strings.isNullOrEmpty(keyword)) jobService.enqueueQuery(q);
        resp.redirect("/");
        return q;
    }
}
