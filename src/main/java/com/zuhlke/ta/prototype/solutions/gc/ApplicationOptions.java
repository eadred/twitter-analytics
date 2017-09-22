package com.zuhlke.ta.prototype.solutions.gc;

import java.io.IOException;
import java.util.Properties;

public class ApplicationOptions {
    public Integer maxWorkers;
    public String tempLocation;
    public String stagingLocation;
    public String projectId;
    public String zone;
    public String dataset;
    public String sourceTable;

    public static ApplicationOptions fromConfig() throws IOException {
        Properties props = new Properties();
        props.load(ApplicationOptions.class.getClassLoader().getResourceAsStream("config.properties"));

        ApplicationOptions result = new ApplicationOptions();
        result.maxWorkers = Integer.parseInt(props.getProperty("maxWorkers"));
        result.tempLocation = props.getProperty("tempLocation");
        result.stagingLocation = props.getProperty("stagingLocation");
        result.projectId = props.getProperty("projectId");
        result.zone = props.getProperty("zone");
        result.dataset = props.getProperty("dataset");
        result.sourceTable = props.getProperty("sourceTable");

        return result;
    }
}
