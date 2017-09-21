package com.zuhlke.ta.prototype.solutions.gc;

import com.google.api.services.bigquery.model.TableRow;
import com.zuhlke.ta.prototype.Query;
import com.zuhlke.ta.sentiment.TwitterSentimentAnalyzerImpl;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;
import java.util.UUID;

public class SentimentDataFlowRunner {
    public void run(Query query) throws IOException {
        DataFlowOptions options = DataFlowOptions.fromConfig();
        run(query, options);
    }

    public void run(Query query, DataFlowOptions options) {
        QueryOptions opts = PipelineOptionsFactory.create().as(QueryOptions.class);
        opts.setTempLocation(options.tempLocation);
        opts.setMaxNumWorkers(options.maxWorkers);
        opts.setStagingLocation(options.stagingLocation);
        opts.setProject(options.projectId);
        opts.setZone(options.zone);
        opts.setJobName(String.format("sentiment-%s", UUID.randomUUID().toString()));
        // opts.setFilesToStage(new ArrayList<>()); // If this isn't set will default to all files on classpath

        Pipeline p = Pipeline.create(opts);

        String queryString = "SELECT message, date FROM camp_exercise.tweets_partial WHERE UPPER(message) CONTAINS UPPER('" + query.keyword + "') LIMIT 1000";
        p.apply(BigQueryIO.read().fromQuery(queryString))
                .apply(ParDo.of(new ToDatedMessage()))
                .apply(ParDo.of(new ToDatedSentiment()))
                .apply(Combine.perKey(new Combiner()))
                .apply(ParDo.of(new ToOutputText()))
                .apply(TextIO.write().to("gs://eadred-dataflow/dummy_out/"));

        DataflowRunner runner = DataflowRunner.fromOptions(opts);

        DataflowPipelineJob job = runner.run(p);

        PipelineResult.State state = job.waitUntilFinish();
    }

    public interface QueryOptions extends DataflowPipelineOptions {
        String getTempLocation();
        void setTempLocation(String value);
    }

    public static class ToDatedMessage extends DoFn<TableRow, DatedMessage> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();

            String message = (String)row.get("message");
            String date = (String)row.get("date");

            c.output(new DatedMessage(date, message));
        }
    }

    public static class ToDatedSentiment extends DoFn<DatedMessage, KV<String, Double>> {
        private TwitterSentimentAnalyzerImpl sa;

        @StartBundle
        public void startBundle(StartBundleContext c) {
            sa = new TwitterSentimentAnalyzerImpl();
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            sa = null;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            DatedMessage msg = c.element();

            c.output(KV.of(msg.getDate(), sa.getSentiment(msg.getMessage())));
        }
    }

    public static class Combiner extends Combine.CombineFn<Double, CombinedSentiment, CombinedSentiment> {

        @Override
        public CombinedSentiment createAccumulator() {
            return new CombinedSentiment(0,0);
        }

        @Override
        public CombinedSentiment addInput(CombinedSentiment accum, Double sentiment) {
            if (sentiment > 0.0) {
                return new CombinedSentiment(accum.getNumPositive() + 1, accum.getNumNegative());
            } else if (sentiment < 0.0) {
                return new CombinedSentiment(accum.getNumPositive(), accum.getNumNegative() + 1);
            } else {
                return accum;
            }
        }

        @Override
        public CombinedSentiment mergeAccumulators(Iterable<CombinedSentiment> iterable) {
            int positive = 0;
            int negative = 0;
            for (CombinedSentiment s : iterable) {
                positive += s.getNumPositive();
                negative += s.getNumNegative();
            }

            return new CombinedSentiment(positive, negative);
        }

        @Override
        public CombinedSentiment extractOutput(CombinedSentiment accum) {
            return accum;
        }
    }

    public static class ToOutputText extends DoFn<KV<String, CombinedSentiment>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, CombinedSentiment> snt = c.element();

            c.output(snt.getKey() + "," + snt.getValue().getNumPositive() + "," + snt.getValue().getNumNegative());
        }
    }
}
