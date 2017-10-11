package com.zuhlke.ta.prototype;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.DeserializationContext;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class InstantDeserializer extends StdDeserializer<Instant> {
    private static final long serialVersionUID = 1L;

    protected InstantDeserializer() {
        super(Instant.class);
    }

    @Override
    public Instant deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        return Instant.parse(jp.readValueAs(String.class), ISODateTimeFormat.basicDateTime());
    }
}