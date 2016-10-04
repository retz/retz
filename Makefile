.PHONY: test build clean inttest rpm deb dist javadoc license client-jar

GRADLE=./gradlew

javadoc:
	LANG=C $(GRADLE) aggregateJavadoc

test:
	$(GRADLE) test

inttest:
	$(GRADLE) test -Dinttest -is

build:
	$(GRADLE) build

clean:
	$(GRADLE) clean

license:
	$(GRADLE) licenseFormatMain licenseFormatTest

## Built packages are to be at retz-{server,client}/build/distributions/retz-{server,client}-*.{rpm,deb}
## Dependencies must be refreshed to prevent wrong packages
rpm:
	$(GRADLE) --refresh-dependencies buildRpm

deb:
	$(GRADLE) --refresh-dependencies buildDeb

client-jar:
	$(GRADLE) :retz-client:shadowJar
