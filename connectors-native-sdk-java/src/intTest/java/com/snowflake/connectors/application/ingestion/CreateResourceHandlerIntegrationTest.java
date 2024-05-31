/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion;

import static com.snowflake.connectors.application.ingestion.CreateResourceHandler.createResource;
import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.INCREMENTAL;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepositoryFactory;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CreateResourceHandlerIntegrationTest extends BaseIntegrationTest {
  private static final String ID = "id";
  private static final Variant RESOURCE_ID = new Variant(Map.of("tableName", "aaa"));
  private static final Variant RESOURCE_METADATA = new Variant(Map.of("aa", "dd", "bb", "eee"));
  private static final String NAME = "name";
  private static final List<IngestionConfiguration<Variant, Variant>>
      INGESTION_CONFIGURATION_OBJECT =
          List.of(
              new IngestionConfiguration<>(
                  "firstConfig",
                  INCREMENTAL,
                  new Variant(Map.of("prop", "value")),
                  INTERVAL,
                  "10m",
                  new Variant(Map.of("table", "name"))),
              new IngestionConfiguration<>(
                  "secondConfig", INCREMENTAL, null, INTERVAL, "20m", null));

  private static final Variant INGESTION_CONFIGURATIONS =
      new Variant(
          List.of(
              Map.ofEntries(
                  Map.entry("id", "firstConfig"),
                  Map.entry("ingestionStrategy", "INCREMENTAL"),
                  Map.entry("scheduleType", "INTERVAL"),
                  Map.entry("scheduleDefinition", "10m"),
                  Map.entry("customIngestionConfiguration", Map.of("prop", "value")),
                  Map.entry("destination", Map.of("table", "name"))),
              Map.ofEntries(
                  Map.entry("id", "secondConfig"),
                  Map.entry("ingestionStrategy", "INCREMENTAL"),
                  Map.entry("scheduleType", "INTERVAL"),
                  Map.entry("scheduleDefinition", "20m"))));

  ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository =
      ResourceIngestionDefinitionRepositoryFactory.create(session, VariantResource.class);
  DefaultIngestionProcessRepository ingestionProcessRepository =
      new DefaultIngestionProcessRepository(session);

  @BeforeEach
  public void cleanupStateBeforeEach() {
    cleanupState();
  }

  @AfterAll
  public static void cleanupStateAfterAll() {
    cleanupState();
  }

  @Test
  public void
      shouldCreateResourceIngestionDefinitionAndIngestionProcess_ForEachIngestionConfigurationWhenResourceIsEnabled() {
    // given
    boolean enabled = true;

    // when
    Variant result =
        createResource(
            session, NAME, RESOURCE_ID, INGESTION_CONFIGURATIONS, ID, enabled, RESOURCE_METADATA);

    // then
    assertThatResponseMap(result.asMap())
        .hasOKResponseCode()
        .hasMessage("Resource created")
        .hasId(ID);

    assertThat(getActualResourceIngestionDefinition())
        .hasId(ID)
        .hasName(NAME)
        .isEnabled(true)
        .hasResourceId(RESOURCE_ID)
        .hasResourceMetadata(RESOURCE_METADATA)
        .hasIngestionConfigurations(INGESTION_CONFIGURATION_OBJECT);
    assertIngestionProcessIsCreatedForEachConfiguration();
  }

  @Test
  public void shouldNotCreateAnyProcessForDisabledResource() {
    // given
    boolean enabled = false;

    // when
    Variant result =
        createResource(
            session, NAME, RESOURCE_ID, INGESTION_CONFIGURATIONS, ID, enabled, RESOURCE_METADATA);

    // then
    assertThatResponseMap(result.asMap())
        .hasOKResponseCode()
        .hasMessage("Resource created")
        .hasId(ID);

    assertThat(getActualResourceIngestionDefinition())
        .hasId(ID)
        .hasName(NAME)
        .isEnabled(false)
        .hasResourceId(RESOURCE_ID)
        .hasResourceMetadata(RESOURCE_METADATA)
        .hasIngestionConfigurations(INGESTION_CONFIGURATION_OBJECT);
    assertNoIngestionProcessIsCreated();
  }

  VariantResource getActualResourceIngestionDefinition() {
    Optional<VariantResource> actualResourceIngestionDefinition =
        resourceIngestionDefinitionRepository.fetch(ID);
    assertThat(actualResourceIngestionDefinition).isPresent();
    return actualResourceIngestionDefinition.get();
  }

  void assertIngestionProcessIsCreatedForEachConfiguration() {
    assertIngestionProcessIsCreated("firstConfig");
    assertIngestionProcessIsCreated("secondConfig");
  }

  void assertIngestionProcessIsCreated(String ingestionConfigurationId) {
    List<IngestionProcess> fetchedProcesses =
        ingestionProcessRepository.fetchAll(ID, ingestionConfigurationId, "DEFAULT");
    assertThat(fetchedProcesses).size().isEqualTo(1);
    IngestionProcess process = fetchedProcesses.get(0);
    assertThat(process).hasStatus("SCHEDULED").hasType("DEFAULT");
  }

  void assertNoIngestionProcessIsCreated() {
    int actualProcessNumber =
        session
            .sql(
                "Select count(*) from state.ingestion_process where"
                    + " resource_ingestion_definition_id = '$ID'")
            .collect()[0]
            .getInt(0);
    assertThat(actualProcessNumber).isZero();
  }

  static void cleanupState() {
    session.sql("TRUNCATE TABLE STATE.RESOURCE_INGESTION_DEFINITION").collect();
    session.sql("TRUNCATE TABLE STATE.INGESTION_PROCESS").collect();
  }
}
