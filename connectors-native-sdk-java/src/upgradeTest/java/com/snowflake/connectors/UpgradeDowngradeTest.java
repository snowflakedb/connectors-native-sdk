package com.snowflake.connectors;

import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class UpgradeDowngradeTest extends BaseNativeSdkUpgradeIntegrationTest {

  @Test
  void shouldUpgradeAndDowngradeApplication() {
    application.upgrade();
    application.downgrade();
  }

  @Test
  void shouldPreserveConfigurationStatusWhenUpgradingApplication() {
    // given
    application.completeWizard();
    var config = application.getConnectorConfiguration();
    var prerequisites = application.getPrerequisites();

    // when
    application.upgrade();

    // then
    assertConnectorStatus(STARTED, FINALIZED);
    assertThatConfigIsEqualTo(config);
    assertThatPrerequisitesAreExactly(prerequisites);

    // when
    application.downgrade();
    var configAfterDowngrade = application.getConnectorConfiguration();
    var prerequisitesAfterDowngrade = application.getPrerequisites();

    // then
    assertConnectorStatus(STARTED, FINALIZED);
    assertThat(configAfterDowngrade).containsExactlyInAnyOrder(config);
    assertThat(prerequisitesAfterDowngrade).containsExactlyInAnyOrder(prerequisites);
  }

  @Test
  void shouldPreserveConfigurationStatusWhenUpgradingDuringWizard() {
    // given
    application.completePrerequisites();
    application.configureConnector();
    application.configureConnection();

    // when
    application.upgrade();

    // then
    assertConnectorStatus(CONFIGURING, CONNECTED);

    // when
    application.configureConnector(); // redo previous step just because we want to update something
    application.finalizeConfiguration();

    // then
    assertConnectorStatus(STARTED, FINALIZED);

    // when
    application.downgrade();

    // then
    assertConnectorStatus(STARTED, FINALIZED);
  }

  @Test
  void shouldPauseAndResumeConnectorOnDifferentVersions() {
    // given
    application.setConnectorStatus(STARTED, FINALIZED);
    application.pause();
    assertConnectorStatus(PAUSED, FINALIZED);
    application.initializeTaskReactorInstance(WAREHOUSE);

    // when
    application.upgrade();
    application.resume();

    // then
    assertConnectorStatus(STARTED, FINALIZED);

    // when
    application.pause();
    assertConnectorStatus(PAUSED, FINALIZED);
    application.downgrade();
    application.resume();

    // then
    assertConnectorStatus(STARTED, FINALIZED);
  }

  @Test
  void shouldNotChangeTaskReactorStateUpgrading() {
    // given
    application.completeWizard();
    application.initializeTaskReactorInstance(WAREHOUSE);
    var taskReactorConfig = application.getTaskReactorConfig();

    // when
    application.upgrade();

    // then
    assertThat(application.getInitializedTaskReactorInstances())
        .containsExactly(Application.TASK_REACTOR_INSTANCE_NAME);
    assertTaskReactorConfig(taskReactorConfig);
  }

  @Test
  void shouldNotChangeTaskReactorStateWhenDowngrading() {
    // given
    application.completeWizard();
    application.upgrade();
    application.initializeTaskReactorInstance(WAREHOUSE);
    var taskReactorConfig = application.getTaskReactorConfig();

    // when
    application.downgrade();

    // then
    assertThat(application.getInitializedTaskReactorInstances())
        .containsExactly(Application.TASK_REACTOR_INSTANCE_NAME);
    assertTaskReactorConfig(taskReactorConfig);
  }
}
