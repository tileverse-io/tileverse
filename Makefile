.PHONY: all
all: install test

TAG=$(shell ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout)

.PHONY: help
help:
	@echo "Tileverse Range Reader - Available targets:"
	@echo ""
	@echo "Build targets:"
	@echo "  clean         - Clean all build artifacts"
	@echo "  compile       - Compile all modules"
	@echo "  package       - Build all modules and create JARs"
	@echo "  install       - Install to local repository"
	@echo ""
	@echo "Code quality targets:"
	@echo "  format        - Format code (Spotless + SortPOM)"
	@echo "  format-java   - Format Java code only (Spotless)"
	@echo "  format-pom    - Sort POM files only"
	@echo "  lint          - Check code formatting without applying changes"
	@echo "  lint-java     - Check Java formatting only"
	@echo "  lint-pom      - Check POM formatting only"
	@echo ""
	@echo "Testing targets:"
	@echo "  test          - Run all tests (unit + integration via verify)"
	@echo "  test-unit     - Run unit tests only (surefire)"
	@echo "  test-it       - Run integration tests only (failsafe)"
	@echo "  perf-test     - Run performance tests"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  build-benchmarks - Build benchmark JAR"
	@echo "  benchmarks    - Run all benchmarks"
	@echo "  benchmarks-file - Run file-based benchmarks only"
	@echo "  benchmarks-gc - Run benchmarks with GC profiling"
	@echo ""
	@echo "Development targets:"
	@echo "  verify        - Run full verification (compile + test + format check)"
	@echo "  quick-build   - Fast build without tests"
	@echo "  dev-setup     - Setup development environment"

.PHONY: clean
clean:
	./mvnw clean

.PHONY: compile
compile:
	./mvnw compile -ntp -T1C

.PHONY: package
package:
	./mvnw package -DskipTests -ntp -T1C

.PHONY: quick-build
quick-build: package

.PHONY: install
install:
	./mvnw install -DskipTests -ntp -T1C

.PHONY: format
format: format-pom format-java

.PHONY: format-pom
format-pom:
	./mvnw sortpom:sort -ntp -T1C

.PHONY: format-java
format-java:
	./mvnw spotless:apply -ntp -T1C

.PHONY: lint
lint: lint-pom lint-java

.PHONY: lint-pom
lint-pom:
	./mvnw sortpom:verify -ntp -T1C

.PHONY: lint-java
lint-java:
	./mvnw -Pqa validate -Dsortpom.skip=true -ntp -T1C

.PHONY: test
test:
	./mvnw verify -ntp -T1C -Dfmt.skip

.PHONY: test-unit
test-unit:
	./mvnw test -ntp -T1C -pl '!tileverse-rangereader/benchmarks' -Dfmt.skip

.PHONY: test-it
test-it:
	./mvnw verify -Dsurefire.skip=true -ntp -T1C -Dfmt.skip

.PHONY: build-benchmarks
build-benchmarks:
	./mvnw package -pl :tileverse-rangereader-benchmarks -ntp

.PHONY: benchmarks
benchmarks: build-benchmarks
	java -jar tileverse-rangereader/benchmarks/target/benchmarks.jar

.PHONY: benchmarks-file
benchmarks-file: build-benchmarks
	java -jar tileverse-rangereader/benchmarks/target/benchmarks.jar FileRangeReader

.PHONY: benchmarks-gc
benchmarks-gc: build-benchmarks
	java -jar tileverse-rangereader/benchmarks/target/benchmarks.jar -prof gc

.PHONY: benchmarks-cloud
benchmarks-cloud:
	./mvnw package -pl tileverse-rangereader-benchmarks -Pall-benchmarks -ntp
	java -jar tileverse-rangereader/benchmarks/target/benchmarks.jar

.PHONY: verify
verify: lint test

.PHONY: dev-setup
dev-setup:
	@echo "Setting up development environment..."
	@echo "Checking Java version..."
	@java -version
	@echo "Checking Maven version..."
	@./mvnw --version
	@echo "Installing dependencies..."
	@$(MAKE) install
	@echo "Running initial format..."
	@$(MAKE) format
	@echo "Development environment setup complete!"

.PHONY: ci
ci: clean verify

# Performance testing targets
.PHONY: perf-test
perf-test:
	./mvnw verify -Dit.test="*PerformanceTest" -Dsurefire.skip=true -ntp

# Documentation targets
.PHONY: javadoc
javadoc:
	./mvnw javadoc:javadoc -ntp

.PHONY: site
site:
	./mvnw site -ntp

# Dependency analysis
.PHONY: deps
deps:
	./mvnw dependency:tree

.PHONY: deps-analyze
deps-analyze:
	./mvnw dependency:analyze

# Show project information
.PHONY: info
info:
	@echo "Project: Tileverse Range Reader"
	@echo "Version: $(TAG)"
	@echo "Java version requirement: 17+"
	@echo "Maven version requirement: 4.0.0-rc-4+"
	@echo ""
	@echo "Modules:"
	@echo "  - src/core: Core interfaces and implementations"
	@echo "  - src/s3: AWS S3 implementation"
	@echo "  - src/azure: Azure Blob Storage implementation"
	@echo "  - src/gcs: Google Cloud Storage implementation"
	@echo "  - src/all: Aggregator module"
	@echo "  - benchmarks: JMH performance benchmarks"
