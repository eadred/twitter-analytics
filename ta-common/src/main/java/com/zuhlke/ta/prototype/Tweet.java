package com.zuhlke.ta.prototype;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@DefaultCoder(AvroCoder.class)
public class Tweet implements Serializable {
    @JsonProperty
    public final long id;

    @JsonProperty
    public final String userId;

    @JsonProperty
    public final String message;

    @JsonProperty
    public final String date;

    @JsonProperty
    public final double sentiment;

    @JsonProperty
    public final String lang;

    public Tweet() {
        // For Jackson
        this(0L, "", "", LocalDate.MIN, "", 0.0);
    }

    public Tweet(long id, String userId, String message, LocalDate date, String lang) {
        this(id, userId, message, date, lang, 0.0);
    }

    public Tweet(long id, String userId, String message, LocalDate date, String lang, double sentiment) {
        this.id = id;
        this.userId = userId;
        this.message = message;
        this.date = date.format(DateTimeFormatter.ISO_LOCAL_DATE);
        this.lang = lang;
        this.sentiment = sentiment;
    }

    @JsonIgnore
    public LocalDate getDate() {
        return LocalDate.parse(date);
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "id=" + id +
                ", userId='" + userId + '\'' +
                ", message='" + message + '\'' +
                ", date=" + date +
                '}';
    }
}
