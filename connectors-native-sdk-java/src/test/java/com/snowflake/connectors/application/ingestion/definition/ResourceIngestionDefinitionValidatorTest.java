/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.GLOBAL;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomIngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResource;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResourceId;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResourceMetadata;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.Destination;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ResourceIngestionDefinitionValidatorTest {

  ResourceIngestionDefinitionValidator validator = new ResourceIngestionDefinitionValidator();

  @Test
  void shouldReturnValidationErrorsWhenMandatoryFieldsAreNotSet() {
    // given
    ResourceIngestionDefinition<?, ?, ?, ?> resource = createInvalidResource();

    // expect
    assertThatExceptionOfType(ResourceIngestionDefinitionValidationException.class)
        .isThrownBy(() -> validator.validate(resource))
        .withMessageStartingWith("Resource validation failed with 7 errors")
        .satisfies(
            exception ->
                assertThat(exception.getValidationErrors())
                    .containsExactlyInAnyOrderElementsOf(
                        List.of(
                            "Field 'id' cannot be empty.",
                            "Field 'name' cannot be empty.",
                            "Field 'resourceId' cannot be empty.",
                            "Field 'ingestionConfiguration.id' cannot be empty.",
                            "Field 'ingestionConfiguration.ingestionStrategy' cannot be empty.",
                            "Field 'ingestionConfiguration.scheduleType' cannot be empty.",
                            "Field 'ingestionConfiguration.scheduleDefinition' cannot be empty.")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0h", "1", "123", "11a", "5p", "d5h", "", "1d4h", "m"})
  void shouldReturnValidationErrorWhenScheduleDefinitionIsInvalid(String scheduleDefinition) {
    // given
    ResourceIngestionDefinition<?, ?, ?, ?> resource =
        createResourceWithScheduleDefinition(INTERVAL, scheduleDefinition);

    // expect
    assertThatExceptionOfType(ResourceIngestionDefinitionValidationException.class)
        .isThrownBy(() -> validator.validate(resource))
        .withMessageStartingWith("Resource validation failed with 1 errors")
        .satisfies(
            exception ->
                assertThat(exception.getValidationErrors())
                    .containsExactly(
                        "Invalid schedule definition '"
                            + scheduleDefinition
                            + "'. Interval schedule definition should consist of a number and one"
                            + " of the letters: d, h, m."));
  }

  @ParameterizedTest
  @ValueSource(strings = {"23h", "23H", "1h", "100m", "100M", "2d", "2D"})
  void shouldPassValidationWhenScheduleDefinitionIsValid(String scheduleDefinition) {
    // given
    CustomResource resource = createResourceWithScheduleDefinition(INTERVAL, scheduleDefinition);

    // expect
    assertThatCode(() -> validator.validate(resource)).doesNotThrowAnyException();
  }

  @Test
  void shouldPassValidationWhenIngestionConfigUsesGlobalSchedule() {
    // given
    CustomResource resource = createResourceWithScheduleDefinition(GLOBAL, null);

    // expect
    assertThatCode(() -> validator.validate(resource)).doesNotThrowAnyException();
  }

  private CustomResource createInvalidResource() {
    return new CustomResource(
        null,
        null,
        true,
        null,
        null,
        null,
        List.of(new IngestionConfiguration<>(null, null, null, null, null, null)));
  }

  private CustomResource createResourceWithScheduleDefinition(
      ScheduleType scheduleType, String scheduleDefinition) {
    return new CustomResource(
        "id",
        "name",
        true,
        "parentId",
        new CustomResourceId("1", 2),
        new CustomResourceMetadata("field1", 333),
        List.of(
            new IngestionConfiguration<>(
                "config_id",
                IngestionStrategy.INCREMENTAL,
                new CustomIngestionConfiguration("prop1"),
                scheduleType,
                scheduleDefinition,
                new Destination("value"))));
  }
}
