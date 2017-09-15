package com.zuhlke.ta.dataflow;

import java.io.IOException;
import java.util.Properties;

public class DataFlowOptions {
    public Integer maxWorkers;
    public String stagingLocation;
    public String projectId;
    public String zone;

    public static DataFlowOptions fromConfig() throws IOException {
        Properties props = new Properties();
        props.load(DataFlowOptions.class.getClassLoader().getResourceAsStream("config.properties"));

        DataFlowOptions result = new DataFlowOptions();
        result.maxWorkers = Integer.parseInt(props.getProperty("maxWorkers"));
        result.stagingLocation = props.getProperty("stagingLocation");
        result.projectId = props.getProperty("projectId");
        result.zone = props.getProperty("zone");

        return result;
    }
}
