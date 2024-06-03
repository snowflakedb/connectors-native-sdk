/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.variant;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.snowflake.snowpark_java.types.Variant;
import java.io.IOException;

/** Jackson JSON serializer for {@link Variant}. */
class VariantSerializer extends StdSerializer<Variant> {

  protected VariantSerializer() {
    super(Variant.class);
  }

  @Override
  public void serialize(Variant value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeRawValue(value.asJsonString());
  }
}
