/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.variant;

import static com.fasterxml.jackson.core.Version.unknownVersion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.NullNode;
import com.snowflake.connectors.util.time.LocalDateDeserializer;
import com.snowflake.connectors.util.time.LocalDateSerializer;
import com.snowflake.connectors.util.time.ZoneIdDeserializer;
import com.snowflake.connectors.util.time.ZoneIdSerializer;
import com.snowflake.snowpark_java.types.Variant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/** Utility class for {@link Variant Snowpark Variant} mapping. */
public class VariantMapper {

  private static final Module VARIANT_MAPPER_MODULE =
      new SimpleModule(
          "variantMapper",
          unknownVersion(),
          Map.of(Variant.class, new VariantDeserializer()),
          List.of(new VariantSerializer()));

  private static final Module DATE_MAPPER_MODULE =
      new SimpleModule(
          "dateMapper",
          unknownVersion(),
          Map.of(
              LocalDate.class, new LocalDateDeserializer(), ZoneId.class, new ZoneIdDeserializer()),
          List.of(new LocalDateSerializer(), new ZoneIdSerializer()));

  private static final ObjectMapper objectMapper =
      new ObjectMapper()
          .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .registerModule(VARIANT_MAPPER_MODULE)
          .registerModule(DATE_MAPPER_MODULE);

  private VariantMapper() {}

  /**
   * Maps the provided {@link Variant} to an instance of the provided class.
   *
   * @param variant variant to map
   * @param clazz class to which the variant will be mapped
   * @param <T> type of the provided class
   * @return an object of a given class, created by mapping the variant
   * @throws VariantMapperException if the provided variant can not be mapped to an instance of the
   *     provided class
   */
  public static <T> T mapVariant(Variant variant, Class<T> clazz) {
    if (isNull(variant)) {
      return null;
    } else if (clazz.isInstance(variant)) {
      return clazz.cast(variant);
    }

    try {
      return objectMapper.readValue(variant.asJsonString(), clazz);
    } catch (JsonProcessingException e) {
      throw new VariantMapperException(
          "Cannot parse json value fetched from database to object", e);
    }
  }

  /**
   * Maps the provided {@link Variant} to an instance of the provided {@link JavaType}.
   *
   * @param variant variant to map
   * @param clazz class to which the variant will be mapped
   * @param <T> type of the provided class
   * @return an object of a given class, created by mapping the variant
   * @throws VariantMapperException if the provided variant can not be mapped to an instance of the
   *     provided class
   */
  public static <T> T mapVariant(Variant variant, JavaType clazz) {
    if (isNull(variant)) {
      return null;
    }

    try {
      return objectMapper.readValue(variant.asJsonString(), clazz);
    } catch (JsonProcessingException e) {
      throw new VariantMapperException(
          "Cannot parse json value fetched from database to object", e);
    }
  }

  /**
   * Maps the provided value to {@link Variant}.
   *
   * @param value value to map
   * @param <T> type of the provided value
   * @return Variant created from the provided value
   * @throws VariantMapperException if the provided value can not be mapped to Variant
   */
  public static <T> Variant mapToVariant(T value) {
    if (value == null) {
      return new Variant((Object) null);
    } else if (value instanceof Variant) {
      return (Variant) value;
    } else if (value instanceof Map) {
      return new Variant(value);
    }

    try {
      return new Variant(objectMapper.writeValueAsString(value));
    } catch (JsonProcessingException e) {
      throw new VariantMapperException("Problem with Mapping value to Variant", e);
    }
  }

  /**
   * Returns whether the provided Variant is null, has a value of null, or has a value of type
   * {@link com.fasterxml.jackson.databind.node.NullNode NullNode}.
   *
   * @param variant Variant to check
   * @return whether the provided Variant is considered to be null
   */
  private static boolean isNull(Variant variant) {
    if (variant == null) {
      return true;
    }

    try {
      var field = Variant.class.getDeclaredField("value");
      field.setAccessible(true);

      var value = field.get(variant);
      return value == null || value instanceof NullNode;
    } catch (Exception e) {
      throw new VariantMapperException("Failed to check if Variant is null", e);
    }
  }
}
