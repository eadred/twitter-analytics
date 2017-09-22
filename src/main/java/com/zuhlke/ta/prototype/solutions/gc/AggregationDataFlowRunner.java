package com.zuhlke.ta.prototype.solutions.gc;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.zuhlke.ta.prototype.Query;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class AggregationDataFlowRunner {
    private final ApplicationOptions options;

    public static final String ResultsTable = "results";
    public static final String ResultsDateColumn = "date";
    public static final String ResultsPositiveColumn = "positive";
    public static final String ResultsNegativeColumn = "negative";

    public AggregationDataFlowRunner(ApplicationOptions options) {
        this.options = options;
    }

    public void run(Query query) {
        QueryOptions opts = PipelineOptionsFactory.create().as(QueryOptions.class);
        opts.setTempLocation(options.tempLocation);
        opts.setMaxNumWorkers(options.maxWorkers);
        opts.setStagingLocation(options.stagingLocation);
        opts.setProject(options.projectId);
        opts.setZone(options.zone);
        opts.setJobName(String.format("sentiment-%s", UUID.randomUUID().toString()));

        Pipeline p = Pipeline.create(opts);

        TableReference tableRef = new TableReference();
        tableRef.setProjectId(options.projectId);
        tableRef.setDatasetId(options.dataset);
        tableRef.setTableId(ResultsTable);

        String queryString = String.format(
                "SELECT sentiment, date FROM %s.%s WHERE sentiment <> 0.0 AND UPPER(message) CONTAINS UPPER('%s')",
                options.dataset,
                options.sourceTable,
                query.getKeyword());

        p.apply(BigQueryIO.read().fromQuery(queryString))
                .apply(ParDo.of(new ToDatedSentiment()))
                .apply(Combine.perKey(new Combiner()))
                .apply(ParDo.of(new ToOutputRow()))
                .apply(BigQueryIO.writeTableRows()
                        .to(tableRef)
                        .withSchema(ToOutputRow.getSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        DataflowRunner runner = DataflowRunner.fromOptions(opts);

        DataflowPipelineJob job = runner.run(p);

        PipelineResult.State state = job.waitUntilFinish();
    }

    public interface QueryOptions extends DataflowPipelineOptions {
        String getTempLocation();
        void setTempLocation(String value);
    }

    public static class ToDatedSentiment extends DoFn<TableRow, KV<String, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();

            Double sentiment = (Double)row.get("sentiment");
            String date = (String)row.get("date");

            c.output(KV.of(date, sentiment));
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

    public static class ToOutputRow extends DoFn<KV<String, CombinedSentiment>, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, CombinedSentiment> snt = c.element();

            TableRow r = new TableRow()
                    .set(ResultsDateColumn, c.element().getKey())
                    .set(ResultsPositiveColumn, c.element().getValue().getNumPositive())
                    .set(ResultsNegativeColumn, c.element().getValue().getNumNegative());

            c.output(r);
        }

        static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();
            fields.add(new TableFieldSchema().setName(ResultsDateColumn).setType("STRING"));
            fields.add(new TableFieldSchema().setName(ResultsPositiveColumn).setType("INTEGER"));
            fields.add(new TableFieldSchema().setName(ResultsNegativeColumn).setType("INTEGER"));
            TableSchema schema = new TableSchema().setFields(fields);
            return schema;
        }
    }
}
