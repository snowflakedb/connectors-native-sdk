package com.snowflake.connectors.sdk.examples.example_github_connector

import com.snowflake.snowpark_java.Session
import spock.lang.Shared
import spock.lang.Specification

class BaseIntegrationSpec extends Specification {

    private static final String APP_NAME_PREFIX = "EXAMPLE_GITHUB_JAVA_CONNECTOR"
    private static final String APP_VERSION = "V_1_0_0" //TODO read from manifest
    static final String API_INTEGRATION = "gh_integration"

    @Shared
    Session session

    int randomPostfix = new Random().nextInt(Integer.MAX_VALUE)

    @Shared
    String secretsDatabaseName = "SECRETS_DB"

    @Shared
    String stageDatabaseName = "${APP_NAME_PREFIX}_STAGE_${randomPostfix}"

    String applicationPackageName = "${APP_NAME_PREFIX}_PACKAGE_${randomPostfix}"

    String applicationInstanceName = "${APP_NAME_PREFIX}_INSTANCE_${randomPostfix}"

    String destinationDatabaseName = "DEST_DB_${randomPostfix}"

    def setupSpec() {
        def path = this.getClass().getResource("/it_config.properties").getPath()
        println("path to config: $path")
        session = Session.builder().configFile(path).create()

        buildConnector()
    }

    def setup() {
        deployConnector()
        installApplication()
    }

    def cleanup() {
        assert session != null
        println("Application instance name: " + applicationInstanceName)
        session.sql("DROP APPLICATION IDENTIFIER('${applicationInstanceName}') CASCADE").collect()
        session.sql("DROP APPLICATION PACKAGE IDENTIFIER('${applicationPackageName}')").collect()
    }

    def cleanupSpec() {
        session.sql("DROP DATABASE IDENTIFIER('${stageDatabaseName}')").collect()
    }

    def buildConnector() {
        def currentDir = currentDir()
        println("Building connector in $currentDir")
        def exitStatus = BuildingHelper.runCommand("make build", currentDir)
        assert exitStatus == 0
    }

    def deployConnector() {
        createStage()
        uploadArtifacts()
        createApplicationPackage()
    }

    def createStage() {
        session.sql("CREATE OR REPLACE DATABASE IDENTIFIER('$stageDatabaseName')").collect()
        session.sql("CREATE STAGE IF NOT EXISTS IDENTIFIER('${stageName()}')").collect()
    }

    def stageName() {
        def stageName = "${stageDatabaseName}.public.artifacts"
        return stageName
    }

    def uploadArtifacts() {
        def directory = currentDir()
        session.file().put("file://${directory.absolutePath}/sf_build/*", "@${stageName()}/${APP_VERSION}", ["AUTO_COMPRESS": "FALSE"]).collect()
    }

    def createApplicationPackage() {
        session.sql("DROP DATABASE IF EXISTS IDENTIFIER('$applicationPackageName')").collect()
        session.sql("CREATE APPLICATION PACKAGE IF NOT EXISTS IDENTIFIER('$applicationPackageName')").collect()
    }

    def installApplication() {
        session.sql("DROP DATABASE IF EXISTS IDENTIFIER('$applicationInstanceName')").collect()
        session.sql("CREATE APPLICATION IDENTIFIER('$applicationInstanceName') FROM APPLICATION PACKAGE IDENTIFIER('$applicationPackageName') USING @${stageName()}/${APP_VERSION}").collect()

        session.sql("grant usage on integration ${API_INTEGRATION} to application IDENTIFIER('${applicationInstanceName}')").collect()
        session.sql("grant usage on database ${secretsDatabaseName} to application IDENTIFIER('$applicationInstanceName')").collect()
        session.sql("grant usage on schema ${secretsDatabaseName}.PUBLIC to application IDENTIFIER('$applicationInstanceName')").collect()
        session.sql("grant read on secret ${secretsDatabaseName}.PUBLIC.GH_TOKEN to application IDENTIFIER('$applicationInstanceName')").collect()
        session.sql("grant create database ON ACCOUNT TO APPLICATION IDENTIFIER('$applicationInstanceName')").collect()
        session.sql("grant usage on warehouse xsmall to application IDENTIFIER('$applicationInstanceName')").collect()
        session.sql("grant execute task on account to application IDENTIFIER('$applicationInstanceName')").collect()
        session.sql("grant execute managed task on account to application IDENTIFIER('$applicationInstanceName')").collect()
        session.sql("USE DATABASE $applicationInstanceName").collect()
    }

    File currentDir() {
        def currentDir = System.getProperty("user.dir")
        if (currentDir.endsWith("/integration-test")) {
            currentDir = currentDir.substring(0, currentDir.lastIndexOf("/integration-test"))
        }
        return new File(currentDir)
    }

    def runQuery(String query) {
        return session.sql(query).collect()
    }
}
