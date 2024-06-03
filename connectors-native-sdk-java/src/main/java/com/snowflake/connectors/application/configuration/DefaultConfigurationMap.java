/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration;

import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;

import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import java.util.Optional;

/**
 * Default implementation of {@link ConfigurationMap}, which uses {@link
 * com.snowflake.connectors.util.variant.VariantMapper#mapVariant(Variant, Class) VariantMapper} for
 * value mapping.
 */
public class DefaultConfigurationMap implements ConfigurationMap {

  private final Map<String, Variant> configuration;

  /**
   * Creates a new {@link DefaultConfigurationMap}, backed by the provided map.
   *
   * @param configuration a map with configuration properties
   */
  public DefaultConfigurationMap(Map<String, Variant> configuration) {
    this.configuration = configuration;
  }

  @Override
  public <T> Optional<T> get(String key, Class<T> clazz) {
    return Optional.ofNullable(configuration.get(key)).map(value -> mapVariant(value, clazz));
  }
}
