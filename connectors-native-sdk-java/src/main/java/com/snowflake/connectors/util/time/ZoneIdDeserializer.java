package com.snowflake.connectors.util.time;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.time.ZoneId;

/** Jackson JSON deserializer for {@link ZoneId}. */
public class ZoneIdDeserializer extends StdDeserializer<ZoneId> {

  /** Creates a new {@link ZoneIdDeserializer}. */
  public ZoneIdDeserializer() {
    super(ZoneId.class);
  }

  @Override
  public ZoneId deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    return ZoneId.of(jsonParser.getText());
  }
}
