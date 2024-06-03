/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.GLOBAL;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static net.snowflake.client.jdbc.internal.io.netty.util.internal.StringUtil.isNullOrEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/** Validator for resource ingestion definitions. */
class ResourceIngestionDefinitionValidator {

  private static final Pattern INTERVAL_SCHEDULE_PATTERN = Pattern.compile("^[1-9][0-9]*[dhm]");
  private static final String FIELD_CANNOT_BE_EMPTY_MESSAGE = "Field '%s' cannot be empty.";

  void validate(ResourceIngestionDefinition<?, ?, ?, ?> resource) {
    List<String> validationErrors = validateMandatoryFields(resource);
    if (resource.getIngestionConfigurations() != null) {
      resource.getIngestionConfigurations().stream()
          .map(this::validateScheduleDefinition)
          .flatMap(Optional::stream)
          .forEach(validationErrors::add);
    }
    if (!validationErrors.isEmpty()) {
      throw new ResourceIngestionDefinitionValidationException(validationErrors);
    }
  }

  private List<String> validateMandatoryFields(ResourceIngestionDefinition<?, ?, ?, ?> resource) {
    List<String> validationErrors = new ArrayList<>();
    if (isNullOrEmpty(resource.getId())) {
      validationErrors.add(fieldCannotBeEmptyMessage("id"));
    }
    if (isNullOrEmpty(resource.getName())) {
      validationErrors.add(fieldCannotBeEmptyMessage("name"));
    }
    if (resource.getResourceId() == null) {
      validationErrors.add(fieldCannotBeEmptyMessage("resourceId"));
    }
    if (isIngestionConfigurationEmptyAndNotInherited(resource)) {
      validationErrors.add(
          "Field 'ingestionConfiguration' cannot be empty when parentId is not set.'");
    }
    if (resource.getIngestionConfigurations() != null) {
      resource
          .getIngestionConfigurations()
          .forEach(
              ingestionConfiguration ->
                  validateIngestionConfiguration(ingestionConfiguration, validationErrors));
    }
    return validationErrors;
  }

  private String fieldCannotBeEmptyMessage(String fieldName) {
    return String.format(FIELD_CANNOT_BE_EMPTY_MESSAGE, fieldName);
  }

  private boolean isIngestionConfigurationEmptyAndNotInherited(
      ResourceIngestionDefinition<?, ?, ?, ?> resource) {
    return resource.getParentId() == null
        && (resource.getIngestionConfigurations() == null
            || resource.getIngestionConfigurations().isEmpty());
  }

  private void validateIngestionConfiguration(
      IngestionConfiguration<?, ?> ingestionConfiguration, List<String> validationErrors) {
    if (isNullOrEmpty(ingestionConfiguration.getId())) {
      validationErrors.add(fieldCannotBeEmptyMessage("ingestionConfiguration.id"));
    }
    if (ingestionConfiguration.getIngestionStrategy() == null) {
      validationErrors.add(fieldCannotBeEmptyMessage("ingestionConfiguration.ingestionStrategy"));
    }
    if (ingestionConfiguration.getScheduleType() == null) {
      validationErrors.add(fieldCannotBeEmptyMessage("ingestionConfiguration.scheduleType"));
    }
    if (ingestionConfiguration.getScheduleDefinition() == null
        && GLOBAL != ingestionConfiguration.getScheduleType()) {
      validationErrors.add(fieldCannotBeEmptyMessage("ingestionConfiguration.scheduleDefinition"));
    }
  }

  private Optional<String> validateScheduleDefinition(
      IngestionConfiguration<?, ?> ingestionConfiguration) {
    if (ingestionConfiguration.getScheduleType() != null
        && ingestionConfiguration.getScheduleType() == INTERVAL) {
      String scheduleDefinition = ingestionConfiguration.getScheduleDefinition();
      boolean isScheduleValid =
          INTERVAL_SCHEDULE_PATTERN.matcher(scheduleDefinition.toLowerCase()).matches();
      if (!isScheduleValid) {
        return Optional.of(
            String.format(
                "Invalid schedule definition '%s'. Interval schedule definition should consist of a"
                    + " number and one of the letters: d, h, m.",
                scheduleDefinition));
      }
    }
    return Optional.empty();
  }
}
