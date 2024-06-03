.PHONY: clean
clean:
	./gradlew clean

.PHONY: test
test: clean
	./gradlew test -i

.PHONY: spotbugs
spotbugs: clean
	./gradlew spotbugsMain spotbugsTest

.PHONY: checkstyle
checkstyle: clean
	./gradlew checkstyleMain checkstyleTest

.PHONY: static-analysis
static-analysis: spotbugs checkstyle
