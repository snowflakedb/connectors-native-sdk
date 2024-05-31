/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.variant;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.snowflake.snowpark_java.types.Variant;
import java.io.IOException;

/** Jackson JSON deserializer for {@link Variant}. */
public class VariantDeserializer extends StdDeserializer<Variant> {

  protected VariantDeserializer() {
    super(Variant.class);
  }

  @Override
  public Variant deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
    JsonNode rootNode = parser.getCodec().readTree(parser);
    return new Variant(rootNode.toString());
  }
}
