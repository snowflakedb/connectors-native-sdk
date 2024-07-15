.PHONY: clean
clean:
	./gradlew clean

.PHONY: test
test: clean
	./gradlew test -i

.PHONY: integration_test
integration_test: clean
	./gradlew intTest -i

.PHONY: application_test
application_test: clean
	./gradlew appTest -i

.PHONY: upgrade_test
upgrade_test: clean
	./gradlew upgradeTest -i

.PHONY: spotbugs
spotbugs: clean
	./gradlew spotbugsMain spotbugsTest spotbugsAppTest spotbugsIntTest

.PHONY: checkstyle
checkstyle: clean
	./gradlew checkstyleMain checkstyleTest checkstyleAppTest checkstyleIntTest

.PHONY: static-analysis
static-analysis: spotbugs checkstyle
