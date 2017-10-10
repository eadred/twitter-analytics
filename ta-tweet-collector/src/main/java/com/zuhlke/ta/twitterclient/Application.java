package com.zuhlke.ta.twitterclient;

import com.zuhlke.ta.prototype.solutions.gc.TweetsImporter;
import com.zuhlke.ta.prototype.solutions.gc.GoogleCloudTweetsImporterImpl;

import java.io.IOException;

public class Application {

    public static void main(String[] args) throws IOException {
        String projectId = args[0];
        String topicName = args[1];
        TweetsImporter importer = new GoogleCloudTweetsImporterImpl(projectId, topicName);
        TwitterClientRunner.runClient(importer);
    }
}
