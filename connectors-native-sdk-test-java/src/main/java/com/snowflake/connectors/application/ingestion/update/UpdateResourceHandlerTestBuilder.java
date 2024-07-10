/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.update;

import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.util.snowflake.TransactionManager;

/**
 * Test builder for the {@link UpdateResourceHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link UpdateResourceValidator}
 *   <li>{@link PreUpdateResourceCallback}
 *   <li>{@link PostUpdateResourceCallback}
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link ResourceIngestionDefinitionRepository}
 *   <li>{@link IngestionProcessRepository}
 *   <li>{@link TransactionManager}
 * </ul>
 */
public class UpdateResourceHandlerTestBuilder extends UpdateResourceHandlerBuilder {

  /**
   * Creates a new, empty {@link UpdateResourceHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public UpdateResourceHandlerTestBuilder() {}

  /**
   * Sets the error helper used to build the test handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public UpdateResourceHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the UpdateResourceValidator used to build the test handler instance. It allows to add
   * connector-specific logic which validates whether a resource can be updated.
   *
   * @param validator custom callback implementation
   * @return this builder
   */
  public UpdateResourceHandlerTestBuilder withUpdateResourceValidator(
      UpdateResourceValidator validator) {
    super.withUpdateResourceValidator(validator);
    return this;
  }

  /**
   * Sets the PreUpdateResourceCallback used to build the test handler instance. It allows to add
   * connector-specific logic which is invoked before a resource is updated.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public UpdateResourceHandlerTestBuilder withPreUpdateResourceCallback(
      PreUpdateResourceCallback callback) {
    super.withPreUpdateResourceCallback(callback);
    return this;
  }

  /**
   * Sets the PostUpdateResourceCallback used to build the test handler instance. It allows to add
   * connector-specific logic which is invoked after a resource is updated.
   *
   * @param callback custom callback implementation
   * @return this builder
   */
  public UpdateResourceHandlerTestBuilder withPostUpdateResourceCallback(
      PostUpdateResourceCallback callback) {
    super.withPostUpdateResourceCallback(callback);
    return this;
  }

  /**
   * Sets the resource ingestion definition repository used to build the test handler instance.
   *
   * @param resourceIngestionDefinitionRepository resource ingestion definition repository
   * @return this builder
   */
  public UpdateResourceHandlerTestBuilder withResourceIngestionDefinitionRepository(
      ResourceIngestionDefinitionRepository<VariantResource>
          resourceIngestionDefinitionRepository) {
    this.resourceIngestionDefinitionRepository = resourceIngestionDefinitionRepository;
    return this;
  }

  /**
   * Sets the ingestion process repository used to build the test handler instance.
   *
   * @param ingestionProcessRepository ingestion process repository
   * @return this builder
   */
  public UpdateResourceHandlerTestBuilder withIngestionProcessRepository(
      IngestionProcessRepository ingestionProcessRepository) {
    this.ingestionProcessRepository = ingestionProcessRepository;
    return this;
  }

  /**
   * Sets the transaction manager used to build the test handler instance.
   *
   * @param transactionManager transaction manager
   * @return this builder
   */
  public UpdateResourceHandlerTestBuilder withTransactionManager(
      TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
    return this;
  }
}
