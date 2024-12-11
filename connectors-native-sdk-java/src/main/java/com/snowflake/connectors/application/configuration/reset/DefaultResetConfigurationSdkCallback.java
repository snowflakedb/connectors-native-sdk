/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.connectors.application.configuration.prerequisites.PrerequisitesRepository;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link ResetConfigurationSdkCallback}. */
class DefaultResetConfigurationSdkCallback implements ResetConfigurationSdkCallback {

  private final ConfigurationRepository configurationRepository;
  private final PrerequisitesRepository prerequisitesRepository;
  private final TransactionManager transactionManager;

  public DefaultResetConfigurationSdkCallback(
      ConfigurationRepository configurationRepository,
      PrerequisitesRepository prerequisitesRepository,
      TransactionManager transactionManager) {
    this.configurationRepository = configurationRepository;
    this.prerequisitesRepository = prerequisitesRepository;
    this.transactionManager = transactionManager;
  }

  DefaultResetConfigurationSdkCallback(Session session) {
    this.configurationRepository = ConfigurationRepository.getInstance(session);
    this.prerequisitesRepository = PrerequisitesRepository.getInstance(session);
    this.transactionManager = TransactionManager.getInstance(session);
  }

  @Override
  public ConnectorResponse execute() {
    transactionManager.withTransaction(
        () -> {
          prerequisitesRepository.markAllPrerequisitesAsUndone();
          configurationRepository.delete(ConfigurationRepository.CONNECTOR_CONFIGURATION_KEY);
          configurationRepository.delete(ConfigurationRepository.CONNECTION_CONFIGURATION_KEY);
        });
    return ConnectorResponse.success();
  }
}
