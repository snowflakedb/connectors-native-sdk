/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import static com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository.EXPRESSION_LIMIT;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomAbstractResource;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResource;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResourceId;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResourceWithEmptyCustomFields;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResourceWithPrivateNoArgsConstructor;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResourceWithThrowingConstructor;
import com.snowflake.connectors.application.ingestion.definition.CustomResourceClasses.CustomResourceWithoutNoArgsConstructor;
import com.snowflake.connectors.common.table.DuplicateKeyException;
import com.snowflake.connectors.common.table.RecordsLimitExceededException;
import com.snowflake.connectors.util.variant.VariantMapperException;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ResourceIngestionDefinitionRepositoryTest extends BaseIntegrationTest {

  ResourceIngestionDefinitionRepository<CustomResource> customResourceRepository =
      ResourceIngestionDefinitionRepositoryFactory.create(session, CustomResource.class);

  @BeforeEach
  void cleanupState() {
    session.sql("TRUNCATE TABLE STATE.RESOURCE_INGESTION_DEFINITION").collect();
  }

  @Test
  void shouldCreateUpdateAndFetchAResourceWithAllCustomData() {
    // given
    var resource = createResource();

    // when
    customResourceRepository.save(resource);

    // then
    var fetchedResource = customResourceRepository.fetch(resource.getId());
    assertThat(fetchedResource).isPresent().hasValue(resource);

    // when
    var updatedResource = createUpdatedResource();
    customResourceRepository.save(updatedResource);

    // then
    var fetchedUpdatedResource = customResourceRepository.fetch(updatedResource.getId());
    assertThat(fetchedUpdatedResource).isPresent().hasValue(updatedResource);
  }

  @Test
  void shouldInsertMultipleAndUpdateMultipleResources() {
    // given
    var id1 = newRandomId();
    var id2 = newRandomId();
    var id3 = newRandomId();

    var resource1 = createResource(id1);
    var resource2 = createResource(id2);
    var resource3 = createResource(id3);

    var updated1 = createUpdatedResource(id1);
    var updated2 = createUpdatedResource(id2);

    // when
    customResourceRepository.saveMany(List.of(resource1, resource2));

    // then
    var savedResources = customResourceRepository.fetchAllById(List.of(id1, id2));
    assertThat(savedResources).containsExactlyInAnyOrder(resource1, resource2);

    // when
    customResourceRepository.saveMany(List.of(updated1, updated2, resource3));

    // then
    var updatedResources = customResourceRepository.fetchAllById(List.of(id1, id2, id3));
    assertThat(updatedResources).containsExactlyInAnyOrder(updated1, updated2, resource3);
  }

  @Test
  void shouldFailToUpdateManyFieldsWhenKeysAreDuplicated() {
    // given
    var id1 = newRandomId();
    var id2 = newRandomId();

    var resource1 = createResource(id1);
    var resource1Duplicate = createResource(id1);
    var resource2 = createResource(id2);

    // expect
    assertThatExceptionOfType(DuplicateKeyException.class)
        .isThrownBy(
            () ->
                customResourceRepository.saveMany(
                    List.of(resource1, resource2, resource1Duplicate)))
        .withMessage(
            String.format(
                "There were duplicated keys in the collection. Duplicated IDs found [%s].", id1));
  }

  @Test
  void shouldInsert1000Records() {
    // given
    var ids = IntStream.range(1, 1001).mapToObj(Integer::toString).collect(toUnmodifiableList());
    var customResources = ids.stream().map(this::createResource).collect(toUnmodifiableList());

    // when
    customResourceRepository.saveMany(customResources);

    // then
    var savedResources = customResourceRepository.fetchAllById(ids);
    assertThat(savedResources).containsExactlyInAnyOrderElementsOf(customResources);
  }

  @Test
  void shouldThrowRecordsLimitExceededException() {
    // given
    List<CustomResource> customResources =
        IntStream.range(1, EXPRESSION_LIMIT + 2)
            .mapToObj(Integer::toString)
            .map(this::createResource)
            .collect(toUnmodifiableList());

    // expect
    assertThatExceptionOfType(RecordsLimitExceededException.class)
        .isThrownBy(() -> customResourceRepository.saveMany(customResources))
        .withMessage(
            "Limit of singular update for many properties is: "
                + EXPRESSION_LIMIT
                + ". Reduce amount of passed keyValues or split them into chunks smaller or equal"
                + " to: "
                + EXPRESSION_LIMIT);
  }

  @Test
  void shouldFetchEnabledResources() {
    // given
    var enabledResources =
        List.of(createEnabledResource(), createEnabledResource(), createEnabledResource());
    customResourceRepository.saveMany(enabledResources);

    // and
    var disabledResources = List.of(createDisabledResource(), createDisabledResource());
    customResourceRepository.saveMany(disabledResources);

    // when
    var fetchedResources = customResourceRepository.fetchAllEnabled();

    // then
    assertThat(fetchedResources)
        .hasSameSizeAs(enabledResources)
        .containsExactlyInAnyOrderElementsOf(enabledResources);
  }

  @Test
  void shouldFetchAllResources() {
    // given
    var resources =
        List.of(createEnabledResource(), createEnabledResource(), createDisabledResource());
    customResourceRepository.saveMany(resources);

    // when
    var enabledResources = customResourceRepository.fetchAll();

    // then
    assertThat(enabledResources)
        .hasSameSizeAs(resources)
        .containsExactlyInAnyOrderElementsOf(resources);
  }

  @Test
  void shouldCountEnabledResources() {
    // given
    var enabledResources =
        List.of(createEnabledResource(), createEnabledResource(), createEnabledResource());
    customResourceRepository.saveMany(enabledResources);

    // and
    var disabledResources = List.of(createDisabledResource(), createDisabledResource());
    customResourceRepository.saveMany(disabledResources);

    // when
    long countEnabled = customResourceRepository.countEnabled();

    // then
    assertThat(enabledResources.size()).isEqualTo(countEnabled);
  }

  @Test
  void shouldFetchAResourceByResourceId() {
    // given
    var resource = createResource();
    customResourceRepository.save(resource);

    // when
    var fetchedResource = customResourceRepository.fetchByResourceId(resource.getResourceId());

    // then
    assertThat(fetchedResource).isPresent().hasValue(resource);
  }

  @Test
  void shouldFetchAResourceWithCustomDataAsNulls() {
    // given
    session
        .sql(
            "INSERT INTO STATE.RESOURCE_INGESTION_DEFINITION (id, name, enabled, parent_id, "
                + "resource_id, resource_metadata, ingestion_configuration, updated_at) SELECT "
                + "'res_id', 'res_name', TRUE, NULL, NULL, NULL, NULL, SYSDATE()")
        .collect();

    // when
    var fetchedResource = customResourceRepository.fetch("res_id");

    // then
    assertThat(fetchedResource)
        .isPresent()
        .hasValue(new CustomResource("res_id", "res_name", true, null, null, null, null));
  }

  @Test
  void shouldSaveAndFetchAResourceWithCustomDataAsNulls() {
    // given
    var resource = createResourceWithNulls();

    // when
    customResourceRepository.save(resource);

    // then
    var fetchedResource = customResourceRepository.fetch(resource.getId());
    assertThat(fetchedResource).isPresent().hasValue(resource);
  }

  @Test
  void shouldSaveAndFetchAResourceWhichDoesNotExtendCustomFields() {
    // given
    var repository =
        ResourceIngestionDefinitionRepositoryFactory.create(
            session, CustomResourceWithEmptyCustomFields.class);
    var resource = createResourceWhichDoesNotExtendCustomFields();

    // when
    repository.save(resource);

    // then
    var fetchedResource = repository.fetch(resource.getId());
    assertThat(fetchedResource).isPresent().hasValue(resource);
  }

  @Test
  void shouldReturnOptionalEmptyWhenResourceWithGivenIdDoesNotExist() {
    // expect
    var fetchedResource = customResourceRepository.fetch("parapaparapapam");
    assertThat(fetchedResource).isEmpty();
  }

  @Test
  void
      shouldThrowExceptionWhenCreatingResourceIngestionDefinitionRepository_IfResourceClassDoesNotDefineNoArgumentConstructor() {
    // expect
    assertThatThrownBy(
            () ->
                ResourceIngestionDefinitionRepositoryFactory.create(
                    session, CustomResourceWithoutNoArgsConstructor.class))
        .isInstanceOf(ResourceIngestionDefinitionInstantiationException.class)
        .hasMessageContaining(
            "Class "
                + CustomResourceWithoutNoArgsConstructor.class
                + " does not have no argument constructor defined.");
  }

  @Test
  void
      shouldThrowExceptionWhenCreatingResourceIngestionDefinitionRepository_IfNoArgumentsConstructorOfResourceClassIsNotPublic() {
    // expect
    assertThatThrownBy(
            () ->
                ResourceIngestionDefinitionRepositoryFactory.create(
                    session, CustomResourceWithPrivateNoArgsConstructor.class))
        .isInstanceOf(ResourceIngestionDefinitionInstantiationException.class)
        .hasMessageContaining(
            "Cannot access no argument constructor of class "
                + CustomResourceWithPrivateNoArgsConstructor.class
                + ". Make sure it is public.");
  }

  @Test
  void
      shouldThrowExceptionWhenCreatingResourceIngestionDefinitionRepository_ifObjectOfResourceClassCannotBeCreatedBecauseConstructorThrowsException() {
    // expect
    assertThatThrownBy(
            () ->
                ResourceIngestionDefinitionRepositoryFactory.create(
                    session, CustomResourceWithThrowingConstructor.class))
        .isInstanceOf(ResourceIngestionDefinitionInstantiationException.class)
        .hasMessage(
            "Cannot instantiate an object of class "
                + CustomResourceWithThrowingConstructor.class
                + ". Constructor throws exception.");
  }

  @Test
  void
      shouldThrowExceptionWhenCreatingResourceIngestionDefinitionRepository_ifResourceClassIsAbstract() {
    // expect
    assertThatThrownBy(
            () ->
                ResourceIngestionDefinitionRepositoryFactory.create(
                    session, CustomAbstractResource.class))
        .isInstanceOf(ResourceIngestionDefinitionInstantiationException.class)
        .hasMessage("Cannot instantiate an object of class " + CustomAbstractResource.class + ".");
  }

  @Test
  void shouldThrowExceptionWhenTryingToFetchAResourceUsingWrongRepository() {
    // given
    var repository =
        ResourceIngestionDefinitionRepositoryFactory.create(
            session, CustomResourceWithEmptyCustomFields.class);

    // and
    var resource = createResource();
    customResourceRepository.save(resource);

    // expect
    assertThatThrownBy(() -> repository.fetch(resource.getId()))
        .isInstanceOf(VariantMapperException.class)
        .hasMessage("Cannot parse json value fetched from database to object");
  }

  @Test
  void shouldThrowValidationExceptionWhenSavingInvalidResource() {
    // given
    var resource = createInvalidResource();

    // expect
    assertThatExceptionOfType(ResourceIngestionDefinitionValidationException.class)
        .isThrownBy(() -> customResourceRepository.save(resource))
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

  private CustomResource createResource() {
    return createResource(newRandomId());
  }

  private CustomResource createResource(String id) {
    return createResource(id, true);
  }

  private CustomResource createEnabledResource() {
    return createResource(newRandomId(), true);
  }

  private CustomResource createDisabledResource() {
    return createResource(newRandomId(), false);
  }

  private CustomResource createResource(String id, boolean enabled) {
    return new CustomResource(
        id,
        "name",
        enabled,
        "parentId",
        new CustomResourceClasses.CustomResourceId("1", 2),
        new CustomResourceClasses.CustomResourceMetadata("field1", 333),
        List.of(
            new IngestionConfiguration<>(
                "config_id",
                IngestionStrategy.INCREMENTAL,
                new CustomResourceClasses.CustomIngestionConfiguration(newRandomId()),
                ScheduleType.CRON,
                "13m",
                new CustomResourceClasses.Destination(newRandomId())),
            new IngestionConfiguration<>(
                "config_id2",
                IngestionStrategy.SNAPSHOT,
                new CustomResourceClasses.CustomIngestionConfiguration("prop2"),
                ScheduleType.INTERVAL,
                "12m",
                new CustomResourceClasses.Destination("value"))));
  }

  private CustomResource createUpdatedResource() {
    return createResource("id");
  }

  private CustomResource createUpdatedResource(String id) {
    return new CustomResource(
        id,
        "name2",
        false,
        "parentId2",
        new CustomResourceClasses.CustomResourceId("2", id.hashCode()),
        new CustomResourceClasses.CustomResourceMetadata("field2", id.hashCode()),
        List.of(
            new IngestionConfiguration<>(
                "config_id2",
                IngestionStrategy.SNAPSHOT,
                new CustomResourceClasses.CustomIngestionConfiguration(newRandomId()),
                ScheduleType.CRON,
                "12m",
                new CustomResourceClasses.Destination("value"))));
  }

  private CustomResourceWithEmptyCustomFields createResourceWhichDoesNotExtendCustomFields() {
    return new CustomResourceWithEmptyCustomFields(
        "id3",
        "name",
        true,
        null,
        new CustomResourceId("1", 2),
        List.of(
            new IngestionConfiguration<>(
                "config_id",
                IngestionStrategy.INCREMENTAL,
                new EmptyImplementation(),
                ScheduleType.CRON,
                "12m",
                new EmptyImplementation())));
  }

  private CustomResource createResourceWithNulls() {
    return new CustomResource(
        "id2",
        "name",
        true,
        null,
        new CustomResourceId("1", 2),
        null,
        List.of(
            new IngestionConfiguration<>(
                "config_id", IngestionStrategy.INCREMENTAL, null, ScheduleType.CRON, "12m", null)));
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

  private String newRandomId() {
    return randomUUID().toString();
  }
}
