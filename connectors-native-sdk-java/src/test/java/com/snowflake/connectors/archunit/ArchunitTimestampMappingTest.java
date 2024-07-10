/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.archunit;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.belongToAnyOf;
import static com.tngtech.archunit.lang.conditions.ArchPredicates.are;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.snowflake.connectors.util.sql.SnowparkFunctions;
import com.snowflake.snowpark_java.Functions;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import java.sql.Timestamp;
import java.time.Instant;

@AnalyzeClasses(packages = "com.snowflake.connectors")
public class ArchunitTimestampMappingTest {

  @ArchTest
  static final ArchRule noClassShouldUseTimestampToInstant =
      noClasses().should().callMethod(Timestamp.class, "toInstant");

  @ArchTest
  static final ArchRule noClassShouldUseFunctionsLit =
      noClasses()
          .that(are(not(belongToAnyOf(SnowparkFunctions.class))))
          .should()
          .callMethod(Functions.class, "lit", Object.class)
          .because("SnowparkFunctions#lit(Object) should be used instead.");

  @ArchTest
  static final ArchRule noClassShouldUseSnowparkFunctionsLitInstant =
      noClasses()
          .should()
          .callMethod(SnowparkFunctions.class, "lit", Instant.class)
          .because(
              "SnowparkFunctions.lit(TimestampUtil.toTimestamp(instant)) should be used instead.");
}
