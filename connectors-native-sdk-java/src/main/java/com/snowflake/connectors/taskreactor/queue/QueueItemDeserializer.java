/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.snowflake.snowpark_java.types.Variant;
import java.io.IOException;
import java.sql.Timestamp;

/** Jackson JSON deserializer for {@link QueueItem}. */
public class QueueItemDeserializer extends JsonDeserializer<QueueItem> {

  @Override
  public QueueItem deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
    JsonNode rootNode = parser.getCodec().readTree(parser);
    return new QueueItem(
        rootNode.get("id").asText(),
        Timestamp.valueOf(rootNode.get("timestamp").asText()),
        rootNode.get("resourceId").asText(),
        false,
        new Variant(rootNode.get("workerPayload").toString()));
  }
}
