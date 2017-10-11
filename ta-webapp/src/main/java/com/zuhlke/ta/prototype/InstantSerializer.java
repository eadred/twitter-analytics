package com.zuhlke.ta.prototype;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonGenerator;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.SerializerProvider;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class InstantSerializer extends StdSerializer<Instant> {
    private static final long serialVersionUID = 1L;

    public InstantSerializer(){
        super(Instant.class);
    }

    @Override
    public void serialize(Instant value, JsonGenerator gen, SerializerProvider sp) throws IOException, JsonProcessingException {
        gen.writeString(value.toString(ISODateTimeFormat.basicDateTime()));
    }
}
