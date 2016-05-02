.PHONY: test build clean inttest rpm deb dist

GRADLE=./gradlew

test:
	$(GRADLE) test

inttest:
	$(GRADLE) inttest

build:
	$(GRADLE) test

rpm:
	$(GRADLE) buildRpm

deb:
	$(GRADLE) buildDeb

clean:
	$(GRADLE) clean

## Currently for in RHEL/CentOS
dist: #clean rpm
	@rm -rf dist
	@mkdir dist
	@cp **/build/distributions/*.rpm dist/
	@sha1sum dist/*.rpm > dist/retz-sha1sum.txt
	@cp -r doc dist/
	@tar czf dist.tar.gz dist
	@rm -rf dist
