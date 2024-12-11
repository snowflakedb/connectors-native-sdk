package com.snowflake.connectors.util.time;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.time.ZoneId;

/** Jackson JSON serializer for {@link ZoneId}. */
public class ZoneIdSerializer extends StdSerializer<ZoneId> {

  /** Creates a new {@link ZoneIdSerializer}. */
  public ZoneIdSerializer() {
    super(ZoneId.class);
  }

  @Override
  public void serialize(
      ZoneId zoneId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeString(zoneId.toString());
  }
}
