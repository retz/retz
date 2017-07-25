.PHONY: test build clean inttest rpm deb dist javadoc license client-jar

GRADLE=./gradlew

javadoc:
	JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8' LANG=C $(GRADLE) aggregateJavadoc

test:
	$(GRADLE) test

inttest:
	$(GRADLE) clean
	@echo "Testing Apache Mesos 1.2"
	$(GRADLE) :retz-inttest:test -Dinttest -is -Dmesos_version=1.2.1-2.0.1
	$(GRADLE) clean
	@echo "Testing Apache Mesos 1.3"
	$(GRADLE) :retz-inttest:test -Dinttest -is -Dmesos_version=1.3.0-2.0.3
	$(GRADLE) clean
	@echo "Testing latest Apache Mesos (could be same as 1.3)"
	$(GRADLE) :retz-inttest:test -Dinttest -is

build:
	$(GRADLE) build jacocoTestReport

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

server-jar:
	$(GRADLE) :retz-server:shadowJar

client-jar:
	$(GRADLE) :retz-client:shadowJar
