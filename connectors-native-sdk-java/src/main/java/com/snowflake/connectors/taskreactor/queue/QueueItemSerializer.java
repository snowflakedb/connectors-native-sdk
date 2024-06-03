/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

/** Jackson JSON serializer for {@link QueueItem}. */
public class QueueItemSerializer extends JsonSerializer<QueueItem> {

  @Override
  public void serialize(QueueItem value, JsonGenerator generator, SerializerProvider serializers)
      throws IOException {
    generator.writeStartObject();
    generator.writeStringField("id", value.id);
    generator.writeStringField("timestamp", value.timestamp.toString());
    generator.writeStringField("resourceId", value.resourceId);
    generator.writeFieldName("workerPayload");
    generator.writeRawValue(value.workerPayload.asJsonString());
    generator.writeEndObject();
  }
}
