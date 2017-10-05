package com.zuhlke.ta.prototype;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.time.LocalDate;

public class TweetTests {

    @Test
    public void Tweet_serialization_round_trip() throws JsonProcessingException, IOException {
        Tweet tweet = new Tweet(5L, "someuser", "some message", LocalDate.now(), 1.0, "en");

        ObjectMapper mapper = new ObjectMapper();

        String tweetString = mapper.writeValueAsString(tweet);

        Tweet tweetDeser = mapper.readValue(tweetString, Tweet.class);

        assertEquals(tweet.id, tweetDeser.id);
        assertEquals(tweet.message, tweetDeser.message);
        assertEquals(tweet.userId, tweetDeser.userId);
        assertEquals(tweet.date, tweetDeser.date);
        assertEquals(tweet.sentiment, tweetDeser.sentiment, 1e-6);
        assertEquals(tweet.lang, tweetDeser.lang);
    }
}
