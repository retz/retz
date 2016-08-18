.PHONY: test build clean inttest rpm deb dist javadoc license

GRADLE=./gradlew

javadoc:
	$(GRADLE) aggregateJavadoc

test:
	$(GRADLE) test

inttest:
	$(GRADLE) inttest

build:
	$(GRADLE) build

rpm:
	$(GRADLE) buildRpm

deb:
	$(GRADLE) buildDeb

clean:
	$(GRADLE) clean

license:
	$(GRADLE) licenseFormatMain licenseFormatTest

## Currently for in RHEL/CentOS
dist: #clean rpm
	@rm -rf dist
	@mkdir dist
	@cp **/build/distributions/*.rpm dist/
	@sha1sum dist/*.rpm > dist/retz-sha1sum.txt
	@cp -r doc dist/
	@tar czf dist.tar.gz dist
	@rm -rf dist
