package com.zuhlke.ta.prototype.solutions.gc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.zuhlke.ta.prototype.Tweet;
import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SentimentDataFlowRunner {
    private final ApplicationOptions options;
    private DataflowPipelineJob job;

    public SentimentDataFlowRunner(ApplicationOptions options) {
        this.options = options;
    }

    public void run() {
        AggregationDataFlowRunner.QueryOptions opts = PipelineOptionsFactory.create().as(AggregationDataFlowRunner.QueryOptions.class);
        opts.setTempLocation(options.tempLocation);
        opts.setMaxNumWorkers(options.maxWorkersSentiment);
        opts.setWorkerMachineType("n1-standard-2");
        opts.setDiskSizeGb(10);
        opts.setStagingLocation(options.stagingLocation);
        opts.setProject(options.projectId);
        opts.setZone(options.zone);
        opts.setJobName(String.format("sentiment-%s", UUID.randomUUID().toString()));

        Pipeline p = Pipeline.create(opts);

        String topicNameFull = "projects/" + options.projectId + "/topics/" + options.topicName;

        TableReference tableRef = new TableReference();
        tableRef.setProjectId(options.projectId);
        tableRef.setDatasetId(options.dataset);
        tableRef.setTableId(options.sourceTable);

        p.apply(PubsubIO.readMessages().fromTopic(topicNameFull))
                .apply(ParDo.of(new ReadTweetFromMsgFn()))
                .apply(ParDo.of(new AnalyzeSentimentFn()))
                .apply(ParDo.of(new TweetToTableRow()))
                .apply(BigQueryIO.writeTableRows().to(tableRef)
                        .withSchema(TweetToTableRow.getSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        DataflowRunner runner = DataflowRunner.fromOptions(opts);

        job = runner.run(p);
    }

    public void cancel() {
        try {
            if (job != null) job.cancel();
        } catch (IOException e) {
            System.err.println("Error cancelling sentiment data flow job");
            System.err.println(e);
        }

    }

    public static class ReadTweetFromMsgFn extends DoFn<PubsubMessage, Tweet> {
        private ObjectMapper mapper;

        public ReadTweetFromMsgFn() {
            mapper = new ObjectMapper();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String payload;
            try {
                payload = new String(c.element().getPayload(), "UTF-8");
            }
            catch (UnsupportedEncodingException e) {
                return;
            }

            Tweet tweet;
            try {
                tweet = mapper.readValue(payload, Tweet.class);
            }
            catch (IOException e) {
                return;
            }

            c.output(tweet);
        }
    }

    public static class AnalyzeSentimentFn extends DoFn<Tweet, Tweet> {
        private static final Logger LOG = LoggerFactory.getLogger(SentimentDataFlowRunner.class);

        private transient TwitterSentimentAnalyzerImpl analyzer;

        @StartBundle
        public void startBundle(StartBundleContext c) {
            LOG.info("Starting bundle");
            if (analyzer == null) {
                LOG.info("Creating sentiment analyzer");
                analyzer = new TwitterSentimentAnalyzerImpl();
            } else {
                LOG.info("Reusing sentiment analyzer");
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Tweet tweet = c.element();

            double sentiment = analyzer.getSentiment(tweet.message);
            Tweet analyzedTweet = new Tweet(tweet.id, tweet.userId, tweet.message, tweet.getDate(), sentiment);

            c.output(analyzedTweet);
        }
    }

    public static class TweetToTableRow extends DoFn<Tweet, TableRow> {

        private static final String ID_COL = "id";
        private static final String USER_ID_COL = "userId";
        private static final String MESSAGE_COL = "message";
        private static final String DATE_COL = "date";
        private static final String SENTIMENT_COL = "sentiment";

        @ProcessElement
        public void processElement(ProcessContext c) {
            Tweet t = c.element();

            TableRow r = new TableRow()
                    .set(ID_COL, t.id)
                    .set(USER_ID_COL, t.userId)
                    .set(MESSAGE_COL, t.message)
                    .set(DATE_COL, t.date)
                    .set(SENTIMENT_COL, t.sentiment);

            c.output(r);
        }

        static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();
            fields.add(new TableFieldSchema().setName(ID_COL).setType("INTEGER"));
            fields.add(new TableFieldSchema().setName(USER_ID_COL).setType("STRING"));
            fields.add(new TableFieldSchema().setName(MESSAGE_COL).setType("STRING"));
            fields.add(new TableFieldSchema().setName(DATE_COL).setType("DATE"));
            fields.add(new TableFieldSchema().setName(SENTIMENT_COL).setType("FLOAT"));
            TableSchema schema = new TableSchema().setFields(fields);
            return schema;
        }
    }
}
