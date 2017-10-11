package com.zuhlke.ta.web;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.base.Strings;
import com.zuhlke.ta.prototype.*;
import com.zuhlke.ta.prototype.solutions.couchbase.CouchbaseResultsStore;
import com.zuhlke.ta.prototype.solutions.gc.*;
import com.zuhlke.ta.prototype.solutions.inmemory.InMemoryResultsStore;
import spark.ModelAndView;
import spark.Request;
import spark.Response;
import spark.template.freemarker.FreeMarkerEngine;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static spark.Spark.get;
import static spark.Spark.post;

public class Application {
    public static void main(String[] args) throws IOException, URISyntaxException {
        String projectId = args[0];
        String dataset = args[1];
        String sourceTable = args[2];

        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
        TweetService tweetService = new GoogleCloudTweetsService(bigquery, dataset, sourceTable);
        ResultsStore resultsStore = getResultsStore(args);
        JobService jobService = new JobService(tweetService, resultsStore);

        FreeMarkerEngine freeMarker = new FreeMarkerEngine();

        get("/", (req, resp) -> homepageData(jobService), freeMarker);
        get("/results/", (req, resp) -> jobService.getResults());
        get("/pending/", (req, resp) -> jobService.getPending());
        post("/jobs/", (req, resp) -> enqueueJob(jobService, req, resp));
    }

    private static ResultsStore getResultsStore(String[] args) {
        if (args.length < 4) return new InMemoryResultsStore();

        return new CouchbaseResultsStore(args[3]);
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
