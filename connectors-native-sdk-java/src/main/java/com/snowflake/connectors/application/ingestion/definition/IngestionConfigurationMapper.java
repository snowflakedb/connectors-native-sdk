/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.snowflake.connectors.common.exception.InvalidInputException;
import com.snowflake.connectors.util.variant.VariantMapperException;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/**
 * Mapper that is used for mapping the ingestion configurations as a raw {@link Variant} to the
 * {@link List} of {@link IngestionConfiguration} objects.
 */
public class IngestionConfigurationMapper {

  /**
   * Mapper main method that maps raw {@link Variant} to the {@link List} of {@link
   * IngestionConfiguration} objects.
   *
   * @param ingestionConfigurations raw ingestion configurations
   * @return list of {@link IngestionConfiguration} object
   */
  public static List<IngestionConfiguration<Variant, Variant>> map(
      Variant ingestionConfigurations) {
    TypeFactory typeFactory = TypeFactory.defaultInstance();
    JavaType ingestionConfigurationType =
        typeFactory.constructParametricType(
            IngestionConfiguration.class, Variant.class, Variant.class);
    var ingestionConfigurationListType =
        typeFactory.constructParametricType(List.class, ingestionConfigurationType);
    try {
      return mapVariant(ingestionConfigurations, ingestionConfigurationListType);
    } catch (VariantMapperException e) {
      throw new InvalidInputException(
          "Provided ingestion configuration has invalid structure and cannot be processed.");
    }
  }
}
