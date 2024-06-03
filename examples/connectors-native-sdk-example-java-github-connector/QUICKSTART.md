# Connector Native Java SDK Example Java Github Connector

## 1. Overview

### Summary
In this tutorial you will learn how to build and deploy a connector using Native Apps framework in Snowflake and Native Connectors SDK Java. 
Provided example application connects to the GitHub API to pull information about issues from the desired repositories.
The following steps will guide you through the whole process starting with the source files 
and finishing with working and configured application inside Snowflake.

### Prerequisites
Before starting this tutorial please be sure that below prerequisites are satisfied:
- Basic knowledge of [Snowflake Native Apps](https://docs.snowflake.com/en/developer-guide/native-apps/native-apps-about)
- Basic knowledge of Java
- Basic knowledge of [Streamlit UI](https://docs.streamlit.io/)
- Snowflake user with `accountadmin` role
- GitHub account with [access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
- MacOS or Linux machine to build a project and run deployment scripts
- Basic knowledge on Pull Based Connectors: [Java](https://quickstarts.snowflake.com/guide/connectors_github_java), [Python](https://quickstarts.snowflake.com/guide/connectors_github_python)

### You will learn
- How to build and deploy Native App based on Connectors Native SDK Java
- How to use features provided by Connectors Native SDK Java

## 2. Prepare your local environment

- Install Java 11
- Clone the [connectors-native-sdk](https://github.com/snowflakedb/connectors-native-sdk) repository
- Install [snowsql](https://docs.snowflake.com/en/user-guide/snowsql)
- Configure `snowsql` to allow using [variables](https://docs.snowflake.com/en/user-guide/snowsql-use#enabling-variable-substitution) (`variable_substitution = True`)
- Configure `snowsql` to [exit on first error](https://docs.snowflake.com/en/user-guide/snowsql-config#exit-on-error) (`exit_on_error = True`)
- Configure `snowsql` [connection](https://docs.snowflake.com/en/user-guide/snowsql-start#defining-named-connections-in-the-configuration-file) named: `native_sdk_connection` or change the CONNECTION environmental variable in the example `examples/connectors-native-sdk-example-java-github-connector/Makefile`

## 3. Project structure

The project contains multiple subdirectories which will be shortly described in the following section.

### Connectors Native SDK Java
The `connectors-native-sdk-java` directory contains all the Native SDK Java code with unit tests and integration tests for the SDK components. 
Because of the nature of the Native Apps inside Snowflake this means not only Java code, but also sql code, which is necessary to create a working application.
The definitions of the database objects can be found inside `src/main/resources` directory. 
Those files are used while creating an application to customize what objects will be available inside the application. 
In this example we use `all.sql` file, which creates objects for all the available features.
This file will be executed during the installation process of the application.
For now this code is not available as jar archive that can be used as a dependency in java project and has to be included as source files.

### Connectors Native SDK Java test
The `connectors-native-sdk-test-java` directory contains source code of a helper library used in unit tests, for example objects used to mock particular components, custom assertions etc.
Those files are neither a part of the library nor the application.

### Example Java GitHub connector
The actual example connector is located inside `examples/connectors-native-sdk-example-java-github-connector` directory.
Inside this directory `app/` contains all the files needed to run the Native App. The `app/streamlit/` directory 
contains source files necessary to run the [Streamlit UI](https://docs.streamlit.io/). The `setup.sql` 
file is run during the application installation and is responsible for creating the necessary database objects. This file will execute
the `all.sql` file mentioned before, as well as some custom sql code.
The `manifest.yml` is the manifest of the Native App and is required to create application package and then the application itself. 
This file specifies application properties, as well as permissions needed by the application.

Additionally, `examples/connectors-native-sdk-example-java-github-connector` directory contains `src/` subdirectory which contains 
custom connector logic, such as implementation of the required classes and customizations of the default SDK components.

### Connectors Native SDK Template 
The template Gradle Java project with inbuilt Connectors Native SDK Java that allows the developer to deploy, install, and run the
sample, mocked source connector right after downloading the template. You can find it inside `templates/connectors-native-sdk-template`. 
The template is filled with some code already which shows how to use the Native SDK for Connectors Java according to the 
connector flow defined by the Native SDK for Connectors. Reach the [official tutorial](https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/tutorials/native_sdk_tutorial) 
that will guide through the whole developer flow starting from cloning the template project, through the implementation 
process of key functionalities, ending with the deployed and running connector in the Snowflake environment!

Other files in the directory are gradle related files, `README.md`, `CONTRIBUTING.md` and `LICENSE` that deliver the
general information about the repository.

## 4. Build, deploy and installation

The `Makefile` in the `examples/connectors-native-sdk-example-java-github-connector/` directory contains convenience scripts to build, deploy and install the connector. 
Those scripts execute specific gradle tasks, execute some shell scripts and run sql commands in Snowflake. 
They require a `snowsql` connection to be defined on the local machine. 
The name of the connection should be provided in the `CONNECTION` environmental variable at the top of the `Makefile`.
Reminder: Connection needs to use `accountadmin` role.

To perform all the needed scripts, one of the 2 commands below will be enough, 
however the following sections will go deeper to explain the whole process and what is happening under the hood.

```shell
cd examples/connectors-native-sdk-example-java-github-connector
make complex_create_app_instance_from_app_version
```

```shell
cd examples/connectors-native-sdk-example-java-github-connector
make complex_create_app_instance_from_version_dir
```

### Build
**To simply run an application the above commands are enough, however if you are interested in the details continue reading.**
Building a Native App is a little bit different from building a usual Java application. 
There are some things that must be done except just building the jar archives from the sources.
Building the application contains the following steps:
- copying sdk components to the target directory
- copying custom internal components to the target directory

**If you run one of the previous commands and want to run step by step commands then run the following command first**

```shell
make drop_application
```

#### Copying sdk components
This step consists of 2 things. First of all, it copies the Connectors Native SDK Java artifact to the target directory, by default - `sf_build`. 
Secondly, it extracts the bundled sql files from the jar archive to the target directory. 
Using those sql files allows user to customize which provided objects will be created during the application installation.
For the first time users customization is not recommended, because omitting objects may cause some features to fail if done incorrectly.
The provided example application uses `all.sql` file, which creates all the provided objects.

To execute this step independently of the whole process execute the following command:
```shell
make copy_sdk_components
```

This step can be executed to get familiarized with the provided sql files, 
however, because Connectors Native SDK Java is currently provided as source code, 
this can be done by going through the source files in the `connectors-native-sdk-java/main/resources/` directory.

#### Copying internal components
This step builds and copies the artifacts from the custom-built code along with all the streamlit files, manifest, and setup files to the target directory. 
This step can be run independently of other steps by running:
```shell
make copy_internal_components
```

### Deployment
To deploy a Native App an application package needs to be created inside Snowflake. 
Then all the files from the target build directory (`sf_build`) need to be copied there. 
Then based on those files a version of the Native App can be created.

Note: Creating version is optional, for development purposes the application can be created from the stage files directly.

#### Preparing application package
The first part of the deployment is creation of the application package if it does not exist yet. 
Then schema and stage are created inside. Performing this step by itself does not upload any files yet, 
but is a good opportunity to check whether the connection to Snowflake was configured correctly.

```shell
make prepare_app_package
```

#### Deploying files
This step copies all the files and subdirectories recursively from the `sf_build` directory to the stage created in the previous step.
The tree structure of the directory is kept and prefixed with the version of the application that is specified in the `Makefile`.
This process might take a moment especially for bigger apps with a lot of custom code and big artifacts.

```shell
make deploy_connector
```

#### Creating version
This step is optional, but depending on whether it was executed or not, different commands must be executed during the installation.
The result of the following command is creation of the version inside the application package.
During the development, when deploy and installation happen more often, 
it is simply faster to skip creating version and then create application instance using the files directly.

```shell
make create_new_version
```

After this step the APPLICATION PACKAGE will be visible in the `App packages` tab in the `Data products` category.

![app_packages.png](assets/app_packages.png)

### Installation
Installation of the application is the last step of the process. 
It creates an application from the application package created previously.
If the version was not created then the following command must be used:
```shell
make create_app_instance_from_version_dir
```

If the version was created then another command is available, however both of those commands should work:
```shell
make create_app_instance_from_app_version
````

#### After the installation
After application is installed successfully it should be visible in the Apps tab inside Snowflake.

![installed_apps.png](assets/installed_apps.png)

## 5. Connector flow

Before we dig into configuring the connector and ingesting the data let's have a quick look at how the connector actually works.
Below you can see all the steps that will be completed in the following parts of this quickstart. 
The starting point will be completing the prerequisites and going through the Wizard.

The Wizard stage of the Connector guides the user through all the required configurations needed by the connector.
The Daily Use stage allows user to view statistics, configure repositories for ingestion and pausing/resuming the connector.

![connector_overview](assets/connector_overview.svg)

## 6. Wizard
 
After entering the application the Wizard Streamlit page will be opened. 
The connector needs some information provided by the user before it can start ingesting data. 
The Wizard will guide you through all the required steps in the application itself, 
but also on the whole Snowflake account and sometimes even the external system that will be the source of the ingested data, in this case GitHub.
After all those steps are finished the connector will be ready to start ingesting the data.

### Prerequisites
The first step of the Wizard is Prerequisites step. This step will provide you a list of things that must be configured outside the Native App.
Completing the prerequisites is not required, 
but it is recommended to ensure smoother configuration process later.
In the case of the example GitHub connector there are 3 things that need to be taken care of before going further:
- preparing a GitHub account
- creating a personal access token for that GitHub account
- configuring external access integration inside Snowflake

For the GitHub related prerequisites please check the [documentation](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token).

#### External Access
Setting up external connectivity objects inside Snowflake requires the GitHub prerequisites to be completed. 
External connectivity objects have to be created on the account level by the `accountadmin`.

In order to access external resources the connector requires 3 objects to be configured:
- [network rule](https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access#creating-a-network-rule-to-represent-the-external-network-location)
- [secrets](https://docs.snowflake.com/en/sql-reference/sql/create-secret)
- [external access integration](https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access)

The following commands create necessary objects. Names can be customized and will be needed during connector Wizard phase later.

```snowflake
CREATE DATABASE GITHUB_SECRETS;

CREATE OR REPLACE NETWORK RULE GH_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST=('api.github.com:443');

CREATE OR REPLACE SECRET < GITHUB_TOKEN > TYPE=GENERIC_STRING SECRET_STRING='< PASTE API TOKEN >';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GITHUB_INTEGRATION
ALLOWED_NETWORK_RULES = (GH_RULE)
ALLOWED_AUTHENTICATION_SECRETS = ('GITHUB_SECRETS.PUBLIC.< GITHUB_TOKEN >')
ENABLED = TRUE;
```

#### Granting privileges to the application

The application needs to have usage the following privileges on the created objects:
- usage on the external access integration
- usage on the database and the schema with the secrets
- read on the secret itself
To know the application instance name please check the `Makefile` or logs from the installation process.

This translates to queries like those:
```snowflake
GRANT USAGE ON INTEGRATION GITHUB_INTEGRATION TO APPLICATION < application name >;

GRANT USAGE ON DATABASE GITHUB_SECRETS TO APPLICATION < application name >;
GRANT USAGE ON SCHEMA GITHUB_SECRETS.PUBLIC TO APPLICATION < application name >;
GRANT READ ON SECRET GITHUB_SECRETS.PUBLIC.GITHUB_TOKEN TO APPLICATION < application name >;
```

![prerequisites.png](assets/prerequisites.png)

### Connector Configuration
Next step of the Wizard is connector configuration. 
This step allows user to grant to application privileges that can be requested through the permissions sdk, 
choose warehouse which will be referenced when scheduling tasks,
and choose destination database and schema for the data that will be ingested.

#### Permissions
Application requires 2 account level permissions to operate: `CREATE DATABASE` and `EXECUTE TASK`. 
The first one is needed to create a destination database for the ingested data. 
This database should be created outside the application package, so that the ingested data can be left intact after application is uninstalled.
Although, this example does not support this feature.
The second one is needed to schedule periodic tasks that will fetch the data from GitHub and save it.

Granting those privileges can be done using the security tab or by pressing the `Grant privileges` button in the connector configuration screen.
The latter one will result in a popup appearing on the screen.

![privileges.png](assets/privileges.png)

#### Warehouse reference
Connector requires a warehouse to run and schedule tasks.
Application will use a warehouse through a reference.
It can be created using the security tab the same as the privileges above or by pressing the `Choose warehouse` button. 
This will result in another popup.

![warehouse_reference.png](assets/warehouse_reference.png)

#### Database and schema
As mentioned before connector requires a database to store ingested data. This database will be created with the schema specified by the user.
Name of the database is up to the user as long as it is unique inside the account.

The completed connector configuration screen will look similar to this one:
![connector_configuratio.png](assets/connector_configuration.png)

### Connection Configuration
Next step of the Wizard is connection configuration. 
This step allows user to provide names of the external access integration and secret that were created during the Prerequisites step of the Wizard.
Once the names of the objects are provided in the designated fields, 
pressing a confirmation button will trigger TEST_CONNECTION procedure inside the connector.
This procedure will try to access [octocat endpoint](https://api.github.com/octocat) in GitHub to check if the external connectivity was configured correctly,
additionally if API token is invalid for some reason this verification should fail.

When it succeeds application will proceed into finalization step.

![connection_configuration.png](assets/connection_configuration.png)

### Configuration Finalization
Finalization is the last step of the Wizard. In this step you will be asked to provide organisation and repository name. 
This repository should be some repository that was configured to be accessible using the provided GitHub API token.
For now, it will be used only for connection validation purposes. 
This is a bit different from the previous step, because test connection procedure only checks if the GitHub API is accessible and the provided token is valid. 
This step ensures that repository provided by user is accessible with the provided GitHub API token. 
It will fail if the token does not have required permissions to access the repository, for example if the repository is private.

Additionally, during this step the database and schema specified in connector configuration phase will be created.

![finalize_configuration.png](assets/finalize_configuration.png)

## 7. Daily Use

After Wizard is completed successfully it is possible to start the regular use of the connector. 
This part will explain:
- how to configure resources to ingest the data
- how the ingestion process works
- how to view statistics of the ingested records
- how to view ingested data
- how to pause and resume connector

### Configuring resources
To configure resources go to the `Data Sync` tab. This tab displays a list of the repositories already configured for ingestion.
When opened for the first time the list should be empty.
To configure a resource type the organisation and repository names in the designated fields, then press `Queue ingestion` button.

For example:
![define_resource.png](assets/define_resource.png)

The definition for a new resource will be saved, and it will be picked up by the scheduler according to the global schedule. 
**It will take some time before the data is ingested and visible in the sink table.**
It will be visible in the table below:

![list_defined_resources.png](assets/list_defined_resources.png)


### Ingestion schedule and status
At the top of the `Data Sync` tab there is a section containing general information about ingestion. 
This section allows user to see the global schedule with which the configured resources will be ingested.
Furthermore, it will be visited again in the Pause/resume section below. Last indicator here shows the current ingestion status.
At first, it will be in `NOT SYNCING` state until first resource is defined. Then it will transition to `SYNCING`, and finally
when at lest one resource finishes successfully it will show the date of last successful ingestion.

![pause.png](assets/pause.png)

### Ingestion process
Ingestion process is handled using a `Scheduler Task` and `Task Reactor` components. 
The scheduler picks up the defined resources according to the global schedule and submits them as `Work Items` to the dispatcher queue.
Then task reactor component called `Dispatcher` picks them up and splits between the defined number of workers. 
Each worker performs the actual ingestion for every item from the queue that it picks up.
Singular ingestion of a resource consists of fetching the data from the endpoints in the GitHub API 
and then saving them in the designated tables in the sink database.
For this example purposes all the data is fetched in every run, which results in new records being added to the table and old records being updated.
Additionally, execution of each `Work Item` includes logging data like start date, end date, number of ingested rows, status etc.
to internal connector tables, which are then used for statistics purposes.

### Viewing statistics
The `Home` screen contains statistics from the past ingestion runs. The data is based on the view called `PUBLIC.AGGREGATED_CONNECTOR_STATS`.
The view aggregates the number of ingested rows based on the hour of the day when it was ingested. 
The data from this view can be retrieved using worksheet and that way it can be aggregated by a time window bigger than an hour.

There is another view that is available through the worksheet. It is called `PUBLIC.CONNECTOR_STATS`. 
Using this data you can see status, start and end date, average rows ingested per seconds and some other information regarding ingestion.

The chart should look like this:

![stats.png](assets/stats.png)

### Viewing ingested data
The ingested data is not visible in the streamlit ui, but it can be viewed through the worksheet by users with `ADMIN` and `DATA_READER` roles.
To view the data you have to go to the worksheets inside Snowflake and just query the database that was configured as the sink during the connector configuration step.
The sink database uses name and schema that were defined during the connector configuration step. The name of the table is ISSUES and the view is called ISSUES_VIEW.

The table contains the following columns:
* ORGANIZATION
* REPOSITORY
* RAW_DATA

The columns in the view are as follows:
* ID
* ORGANIZATION
* REPOSITORY
* STATE
* TITLE
* CREATED_AT
* UPDATED_AT
* ASSIGNEE

Those fields are extracted from the raw_data inside the original table.

To see the data use the below query:

```snowflake
SELECT * FROM SINK_DATABASE.SINK_SCHEMA.ISSUES;

SELECT * FROM SINK_DATABASE.SINK_SCHEMA.ISSUES_VIEW;
```

### Pausing and resuming
The connector can be paused and resumed, whenever desired. To do so just click the `Pause` button in the `Data Sync` tab. 
When pausing is triggered the underlying scheduling and work execution mechanism is disabled. However, the currently processing tasks
will finish before the connector actually goes into `PAUSED` state. Because of that, it can take some time before it happens.

To resume the connector you just have to press `Resume` button that will be displayed in place of the `Pause` button. 
This will resume the scheduling task which will start queueing new `Work Items`.

![pause.png](assets/pause.png)

### Settings tab
After configuration one more tab called `Settings` becomes available. This tab allows the user to see current connector and connection configurations.
The data from this tab is extracted from the underlying configuration table and is read only.

![connector_config_settings.png](assets/connector_config_settings.png)

![connection_config_settings.png](assets/connection_config_settings.png)

### Troubleshooting
If Connector encounters any problems they will be visible in the `event table` logs if it is enabled in the account.
More on the enabling and using `event table` and logging in Native Apps can be found in the documentation:
* [Set up logging and event sharing for an application](https://docs.snowflake.com/en/developer-guide/native-apps/setting-up-logging-and-events)
* [Enable logging and event sharing for an application](https://other-docs.snowflake.com/en/native-apps/consumer-enable-logging)

## 8. Customization

Connectors Native SDK Java provides components to build a fully customizable application. 
Currently, it is possible to customize the behavior of the application in 2 ways:
- overwriting internal sql procedures
- using builders to overwrite whole `Handlers` with custom implementations of the underlying interfaces

More information on those topics will be covered in the [Snowflake Native SDK for Connectors documentation](https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/index).

## 9. Cleanup

After the tutorial is completed you can either pause the connector as explained in the Daily Use section 
or completely remove it using the convenience script
```shell
make drop_application
```
If this part is skipped then even the example connector will generate credit consumption.

## 10. Integration tests

Connectors Native SDK Java module comes with integration tests bundled. 
Those tests build the SDK module and deploy it using mocked empty application.
The test cases are taking significant time to complete because they require the whole Connector to be built and deployed to Snowflake.
To run those tests you need to specify the Snowflake connection credentials in the file that is located in the `.env` directory.
When the connection file is provided run:

```shell
make test
```
