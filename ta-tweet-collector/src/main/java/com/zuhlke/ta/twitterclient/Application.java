package com.zuhlke.ta.twitterclient;

import com.zuhlke.ta.prototype.solutions.gc.ApplicationOptions;
import com.zuhlke.ta.prototype.solutions.gc.TweetsImporter;
import com.zuhlke.ta.prototype.solutions.gc.GoogleCloudTweetsImporterImpl;

import java.io.IOException;

public class Application {

    public static void main(String[] args) throws IOException {
        ApplicationOptions options = ApplicationOptions.fromConfig();
        TweetsImporter importer = new GoogleCloudTweetsImporterImpl(options);
        TwitterClientRunner.runClient(importer);
    }
}
