# Native SDK for Connectors

Data is fuel for data clouds. The journey of data begins with transferring it into Snowflake, because of that this is
essential step for customers.

There are many various ways of importing the data to the Snowflake environment. One of them is by using connectors.
A connector is an application that allows data flow from an external source system into Snowflake. A Native Connector
is a connector built on the foundations of the [Snowflake Native App Framework][Native Apps docs].

**In order to accelerate and standardize the process of developing new Native Connectors, Snowflake would like to introduce
the Native SDK for Connectors which is a set of libraries, developing tools, example connectors and connector template
projects.**

## Key components

In the diagram below, you can find key components that are listed with a brief description below the diagram.

![Repository Components Diagram](./.assets/components_diagram.png)

### Native SDK for Connectors Java

Native SDK for Connectors Java is a library that is distributed through the [Maven Central Repository][SDK in Maven Central].
The library significantly helps developers in building Snowflake Native Applications of connectors' type. The SDK
consists of both Java and sql components. Sql components can be found in the `.sql` files placed in the resources
directory. They provide the definitions of the database objects, that are created during the installation of the
Connector. Java components mainly serve as procedure handlers. However, among these Java classes, there also are some
helper/util classes that provide useful tools to tackle most common use cases. The database objects and Java classes
create a coherent whole that makes managing the state of the Connector much easier and lets the developers focus on the
implementation of the specific external source ingestion logic which also is easier to do with the SDK.

Reach the [official documentation][Native SDK official docs] and [java docs][Native SDK official java docs] to learn more.

#### Task Reactor

This major component is built into Native SDK for Connectors Java library. It consists of `.sql` files and Java classes
embedded into the library that helps in managing and executing work asynchronously, mostly the work related to data
ingestion. This component offers stability and scalability in work executing. Although this component is dedicated to
orchestrating the ingestion tasks, it also can be used for executing non-ingestion tasks that require an asynchronous
way of the execution.

Learn more from the [official documentation][Task Reactor official docs]

### Native SDK for Connectors Java Test

A separate Java library distributed through the [Maven Central Repository][SDK Test in Maven Central] which consists
of Java utils helping in testing the developed Connectors. These utils include custom assertions and in-memory objects
(mocks) for Java classes used in the Native SDK for Connectors Java library.

### Connectors Native SDK Template

The template Gradle Java project with inbuilt Native Java SDK that allows the developer to deploy, install, and run the
sample, mocked source connector right after downloading the template. The template is filled with some code already which
shows how to use the Native SDK for Connectors Java according to the connector flow defined by the Native SDK for Connectors.
Reach the [official tutorial][Template tutorial] that will guide through the whole developer flow starting
from cloning the template project, through the implementation process of key functionalities, ending with the deployed
and running connector in the Snowflake environment!

### Example connectors

These are example projects of connectors that provide the general information on how the connectors, as Native Applications,
should be created and deployed to the Snowflake environment. Among these connectors there is the one that is built on the
top of the [Connectors Native SDK Template project](#connectors-native-sdk-template), to show in practice how the template
should be used to develop a new connector project. There are also some basic examples of connectors that show the
general concept of the native connectors:

* push-based connectors
* basic pull-based connectors written in Python
* basic pull-based connectors written in Java

Keep in mind that these basic connectors do not use the [Native SDK for Connectors Java library](#native-sdk-for-connectors-java).

### Configuring Snowflake CLI for templates and examples

In order to use the templates and examples, it is required to configure the Snowflake CLI. The configuration file can be
found in root directory of the repository. The file is named [Snowflake CLI configuration file path] ./deployment/snowflake.toml
The file should be fulfilled with the required data depending on users Snowflake credentials. More about
configuring that file can be found in the [Snowflake CLI documentation] [Snowflake CLI docs].

## Structure of the repository

* [Native SDK for Connectors Java](#native-sdk-for-connectors-java) - [connectors-native-sdk-java][Native SDK for Connectors Java path]
* [Native SDK for Connectors Java Test](#native-sdk-for-connectors-java-test) - [connectors-native-sdk-java-test][Native SDK for Connectors Java Test path]
* [Connectors Native SDK Template](#connectors-native-sdk-template) - [connectors-native-sdk-template][Connectors Native SDK Template path]
* [Example Connectors](#example-connectors) - [examples][examples]
    * Connectors Native SDK Example GitHub Java Connector - [examples/connectors-native-sdk-example-java-github-connector][Connectors Native SDK Example GitHub Java Connector path]
    * Example GitHub Java Connector - [examples/examples-basic/example-github-java-connector][Example GitHub Java Connector path]
    * Example GitHub Python Connector - [examples/examples-basic/example-github-python-connector][Example GitHub Python Connector path]
    * Example Push-Based Java Connector - [examples/examples-basic/example-push-based-java-connector][Example Push-Based Java Connector path]

## Contributing
Please refer to [CONTRIBUTING.md][contributing].

## License
Please refer to [LICENSE][license]

[Native SDK official docs]: https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/about-connector-sdk
[Native SDK official java docs]: https://docs.snowflake.com/developer-guide/native-apps/connector-sdk/java/index.html
[Task Reactor official docs]: https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/using/task_reactor
[SDK in Maven Central]: https://central.sonatype.com/artifact/com.snowflake/connectors-native-sdk
[SDK Test in Maven Central]: https://central.sonatype.com/artifact/com.snowflake/connectors-native-sdk-test
[Template tutorial]: https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/tutorials/native_sdk_tutorial
[Native Apps docs]: https://docs.snowflake.com/en/developer-guide/native-apps/native-apps-about
[Snowflake CLI docs]: https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/connecting/specify-credentials

[contributing]: ./CONTRIBUTING.md
[license]: ./LICENSE
[examples]: ./examples
[Native SDK for Connectors Java path]: ./connectors-native-sdk-java
[Native SDK for Connectors Java Test path]: ./connectors-native-sdk-test-java
[Connectors Native SDK Template path]: ./templates/connectors-native-sdk-template
[Connectors Native SDK Example GitHub Java Connector path]: ./examples/connectors-native-sdk-example-java-github-connector
[Example GitHub Java Connector path]: ./examples/examples-basic/example-github-java-connector
[Example GitHub Python Connector path]: ./examples/examples-basic/example-github-python-connector
[Example Push-Based Java Connector path]: ./examples/examples-basic/example-push-based-java-connector
[Snowflake CLI configuration file path]: ./deployment/snowflake.toml
