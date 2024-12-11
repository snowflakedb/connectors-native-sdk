package com.snowflake.connectors;

import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfiguration;
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfigurationStrategy;

public class CustomParallelExecutionStrategy
    implements ParallelExecutionConfigurationStrategy, ParallelExecutionConfiguration {

  private static final int TARGET_CONCURRENCY = 1;

  @Override
  public int getParallelism() {
    return TARGET_CONCURRENCY;
  }

  @Override
  public int getMinimumRunnable() {
    return TARGET_CONCURRENCY;
  }

  @Override
  public int getMaxPoolSize() {
    return TARGET_CONCURRENCY;
  }

  @Override
  public int getCorePoolSize() {
    return TARGET_CONCURRENCY;
  }

  @Override
  public int getKeepAliveSeconds() {
    return 30;
  }

  @Override
  public ParallelExecutionConfiguration createConfiguration(
      ConfigurationParameters configurationParameters) {
    return this;
  }
}
