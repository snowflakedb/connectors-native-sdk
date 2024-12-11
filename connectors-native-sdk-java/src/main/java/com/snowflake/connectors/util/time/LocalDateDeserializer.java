package com.snowflake.connectors.util.time;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.time.LocalDate;

/** Jackson JSON deserializer for {@link LocalDate}. */
public class LocalDateDeserializer extends StdDeserializer<LocalDate> {

  /** Creates a new {@link LocalDateDeserializer}. */
  public LocalDateDeserializer() {
    super(LocalDate.class);
  }

  @Override
  public LocalDate deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    return LocalDate.parse(jsonParser.getText());
  }
}
