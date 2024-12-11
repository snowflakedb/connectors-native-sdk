package com.snowflake.connectors.util.time;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.time.LocalDate;

/** Jackson JSON serializer for {@link LocalDate}. */
public class LocalDateSerializer extends StdSerializer<LocalDate> {

  /** Creates a new {@link LocalDateSerializer}. */
  public LocalDateSerializer() {
    super(LocalDate.class);
  }

  @Override
  public void serialize(
      LocalDate localDate, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeString(localDate.toString());
  }
}
