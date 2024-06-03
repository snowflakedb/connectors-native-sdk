# Native SDK for Connectors Test Java

Native SDK for Connectors Test Java is a library that is distributed through the [Maven Central Repository][SDK Test in Maven Central].
The main goal of the library is to help developers with testing their connectors that use the [Native SDK for Connectors
Java library][Native SDK for Connectors Java readme]. Moreover, the test library is used in the SDK testes.

The key motivation behind the test library is to provide `inMemory` objects that can be used as mocks for classes that 
otherwise require connection to Snowflake. Another important use case was to provide custom assertions for Java 
representation of Snowflake database objects (e.g. task definition) and for custom SDK objects (e.g. connector 
configuration). Moreover, the test library delivers test builders for procedure handlers to allow the user fully 
customize handlers dependencies.

## Provided components

The Native SDK for Connectors Java Test library provides following components:

### Mockups

Procedure handlers provided in the Native SDK for Connectors Java library most often have to perform operations in the 
Snowflake environment. Objects that handle those operations require a `Snowpark Session` object and because of this, it's
difficult to mock them in order to use these objects in unit tests. To make SDK components unit-testable, the SDK test 
library provides mock objects for repositories, services and other low-level classes like database table representations. 
These objects perform operations on in-memory data structures that can be easily set up according to particular test 
conditions.

Mentioned objects can be used like in the example below:

```java
var customResourceRepository = new InMemoryDefaultResourceIngestionDefinitionRepository();
var key = "test_key";

var resource = createResource(key);
customResourceRepository.save(resource);

var result = customResourceRepository.fetch(key);
```

or:

```java
var table = new InMemoryDefaultKeyValueTable();
var repository = new DefaultConfigurationRepository(table);
var connectorService = new DefaultConnectorConfigurationService(repository);
```

### Custom assertions

The custom assertions allow developer to make assertions over some of the Snowflake database objects and over objects 
delivered by the SDK library. Assertions provided in the library are based on the [AssertJ fluent assertions][AssertJ fluent assertions doc]. 
All the provided assertions have a fabrication method implemented inside the NativeSdkAssertions class, furthermore, 
this class inherits all the original AssertJ fabrication methods, so only one import is needed to use both custom and 
base assertions.

Check the [NativeSdkAssertions.java][NativeSdkAssertions.java path] in order to check, which types of assertions are 
offered by the library.

### Test builders for handlers

Most of the procedure handlers in Java are created with the usage of builder. Some of their dependencies can not be customized
in the standard builder. In order to let developers fully customize handler dependencies to perform unit tests on them, 
the SDK test library provides test builder. For more information on using Builders check documentation about the 
[builders customization][Builders customization docs].

Test builders are helper objects similar to the Builders used when customizing SDK components. However, they expose all 
the internal services to override. For more information on using Builders check customization documentation.

## How to use

Here are the methods of using the SDK test library in the new project.

### Library in Maven Central

The library is available in the [Maven Central Repository][SDK Test in Maven Central]. You can add it as a dependency to 
your project if you start it from scratch.

### Connectors Native SDK Template

The test library is build into the [Connectors Native SDK Template project][Connectors Native SDK Template path] which 
allows the developer to start the project with already prepared common code. It's a recommended way of starting a new 
project with the Native SDK for Connectors Java library and its test library.


[SDK Test in Maven Central]: TODO:add_sdk_test_in_maven_central_url
[AssertJ fluent assertions doc]: https://assertj.github.io/doc/
[Builders customization docs]: TODO:add_url_to_builder_customization_docs

[Native SDK for Connectors Java readme]: TODO:path_to_SDK_README
[NativeSdkAssertions.java path]: src/main/java/com/snowflake/connectors/common/assertions/NativeSdkAssertions.java
[Connectors Native SDK Template path]: TODO:add_path_to_readme
