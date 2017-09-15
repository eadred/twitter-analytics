package com.zuhlke.ta.dataflow;

import com.zuhlke.ta.common.Query;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class SentimentDataFlowRunner {
    public void run(Query query) throws IOException {
        DataFlowOptions options = DataFlowOptions.fromConfig();
        run(query, options);
    }

    public void run(Query query, DataFlowOptions options) {
        DataflowPipelineOptions opts = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        opts.setMaxNumWorkers(options.maxWorkers);
        opts.setStagingLocation(options.stagingLocation);
        opts.setProject(options.projectId);
        opts.setZone(options.zone);
        opts.setJobName(String.format("sentiment-%s", UUID.randomUUID().toString()));
        // opts.setFilesToStage(new ArrayList<>()); // If this isn't set will default to all files on classpath

        // TODO Create the Pipeline and run it
    }
}
