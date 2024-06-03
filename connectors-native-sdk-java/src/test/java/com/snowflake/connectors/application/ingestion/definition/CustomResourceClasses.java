/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import java.util.List;
import java.util.Objects;

public class CustomResourceClasses {

  public static class CustomResource
      extends ResourceIngestionDefinition<
          CustomResourceId, CustomResourceMetadata, CustomIngestionConfiguration, Destination> {

    public CustomResource(
        String id,
        String name,
        boolean enabled,
        String parentId,
        CustomResourceId resourceId,
        CustomResourceMetadata resourceMetadata,
        List<IngestionConfiguration<CustomIngestionConfiguration, Destination>>
            customIngestionConfigurations) {
      super(
          id, name, enabled, parentId, resourceId, resourceMetadata, customIngestionConfigurations);
    }

    public CustomResource() {}
  }

  public static class CustomResourceWithEmptyCustomFields
      extends ResourceIngestionDefinition<
          CustomResourceId, EmptyImplementation, EmptyImplementation, EmptyImplementation> {

    public CustomResourceWithEmptyCustomFields() {}

    public CustomResourceWithEmptyCustomFields(
        String id,
        String name,
        boolean enabled,
        String parentId,
        CustomResourceId resourceId,
        List<IngestionConfiguration<EmptyImplementation, EmptyImplementation>>
            ingestionConfigurations) {
      super(
          id,
          name,
          enabled,
          parentId,
          resourceId,
          new EmptyImplementation(),
          ingestionConfigurations);
    }
  }

  public static class CustomResourceWithoutNoArgsConstructor
      extends ResourceIngestionDefinition<
          CustomResourceId, CustomResourceMetadata, CustomIngestionConfiguration, Destination> {
    public CustomResourceWithoutNoArgsConstructor(String xxx) {}
  }

  public static class CustomResourceWithPrivateNoArgsConstructor
      extends ResourceIngestionDefinition<
          CustomResourceId, CustomResourceMetadata, CustomIngestionConfiguration, Destination> {
    private CustomResourceWithPrivateNoArgsConstructor() {}
  }

  public static class CustomResourceWithThrowingConstructor
      extends ResourceIngestionDefinition<
          CustomResourceId, CustomResourceMetadata, CustomIngestionConfiguration, Destination> {
    public CustomResourceWithThrowingConstructor() {
      throw new RuntimeException("error");
    }
  }

  public abstract static class CustomAbstractResource
      extends ResourceIngestionDefinition<
          CustomResourceId, CustomResourceMetadata, CustomIngestionConfiguration, Destination> {
    public CustomAbstractResource() {}
  }

  public static class CustomResourceId {
    private String property1;
    private int property2;

    public CustomResourceId() {}

    public CustomResourceId(String property1, int property2) {
      this.property1 = property1;
      this.property2 = property2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CustomResourceId that = (CustomResourceId) o;
      return property2 == that.property2 && Objects.equals(property1, that.property1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property1, property2);
    }

    public String getProperty1() {
      return property1;
    }

    public int getProperty2() {
      return property2;
    }
  }

  public static class CustomIngestionConfiguration {
    private String property1;

    public CustomIngestionConfiguration() {}

    public CustomIngestionConfiguration(String property1) {
      this.property1 = property1;
    }

    public String getProperty1() {
      return property1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CustomIngestionConfiguration that = (CustomIngestionConfiguration) o;
      return Objects.equals(property1, that.property1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property1);
    }
  }

  public static class CustomResourceMetadata {
    private String field1;
    private int field2;

    public CustomResourceMetadata() {}

    public CustomResourceMetadata(String field1, int field2) {
      this.field1 = field1;
      this.field2 = field2;
    }

    public String getField1() {
      return field1;
    }

    public int getField2() {
      return field2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CustomResourceMetadata that = (CustomResourceMetadata) o;
      return field2 == that.field2 && Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2);
    }
  }

  public static class Destination {
    private String field;

    public Destination() {}

    public Destination(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Destination that = (Destination) o;
      return Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field);
    }
  }
}
