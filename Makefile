# Makefile for CascadeStore

# Variables
JAVA_VERSION = 17
MAVEN = mvn
MAVEN_OPTS = -Dmaven.test.skip=false

# Default target
.PHONY: all
all: clean compile test

# Clean the project
.PHONY: clean
clean:
	$(MAVEN) clean

# Compile the project
.PHONY: compile
compile:
	$(MAVEN) compile

# Run tests
.PHONY: test
test:
	$(MAVEN) test

# Package the project
.PHONY: package
package:
	$(MAVEN) package

# Install the project
.PHONY: install
install:
	$(MAVEN) install

# Run a specific test class
.PHONY: test-class
test-class:
	@if [ -z "$(CLASS)" ]; then \
		echo "Usage: make test-class CLASS=io.cascadestore.lsm.CascadeStoreTest"; \
	else \
		$(MAVEN) test -Dtest=$(CLASS); \
	fi

# Run a specific test method
.PHONY: test-method
test-method:
	@if [ -z "$(CLASS)" ] || [ -z "$(METHOD)" ]; then \
		echo "Usage: make test-method CLASS=io.cascadestore.lsm.CascadeStoreTest METHOD=testPutAndGet"; \
	else \
		$(MAVEN) test -Dtest=$(CLASS)#$(METHOD); \
	fi

# Run benchmark tests
.PHONY: test-benchmark
test-benchmark:
	$(MAVEN) test -Dtest="io.cascadestore.lsm.benchmark.**"

# Run smoke tests
.PHONY: test-smoke
test-smoke:
	$(MAVEN) test -Dtest="io.cascadestore.lsm.smoke.**"

# Run stress tests
.PHONY: test-stress
test-stress:
	$(MAVEN) test -Dtest="io.cascadestore.lsm.stress.**"

# Run integration tests
.PHONY: test-it
test-it:
	$(MAVEN) test -Dtest="io.cascadestore.lsm.it.**"

# YCSB macro benchmark (requires Java + Maven; clones YCSB on first run)
.PHONY: ycsb-dryrun ycsb-matrix-dryrun
ycsb-dryrun:
	./scripts/run-ycsb.sh all workloada-dryrun LEVEL_TIERED

ycsb-matrix-dryrun:
	./scripts/run-ycsb-matrix.sh workloada-dryrun

.PHONY: ycsb-workloada ycsb-workloada-matrix
ycsb-workloada:
	THREADS=1 RECORDCOUNT=100000 OPERATIONCOUNT=100000 MEMTABLE_MB=256 \
		./scripts/run-ycsb-matrix.sh workloada 100000 100000

ycsb-workloada-matrix:
	./scripts/run-ycsb-matrix.sh matrix

# Run all excluded tests
.PHONY: test-all-excluded
test-all-excluded: test-benchmark test-smoke test-stress test-it

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all              - Clean, compile, and test the project"
	@echo "  clean            - Clean the project"
	@echo "  compile          - Compile the project"
	@echo "  test             - Run all tests (excluding benchmark, smoke, stress, and integration tests)"
	@echo "  test-benchmark   - Run benchmark tests"
	@echo "  test-smoke       - Run smoke tests"
	@echo "  test-stress      - Run stress tests"
	@echo "  test-it          - Run integration tests"
	@echo "  ycsb-dryrun      - YCSB Workload A dry-run (load + run, LEVEL_TIERED)"
	@echo "  ycsb-matrix-dryrun - YCSB dry-run for all three compaction strategies"
	@echo "  ycsb-workloada       - Workload A 100k, single-thread, baseline profile"
	@echo "  ycsb-workloada-matrix - Workload A 100k × 3 compaction strategies"
	@echo "  test-all-excluded - Run all excluded tests"
	@echo "  package          - Package the project"
	@echo "  install          - Install the project"
	@echo "  test-class       - Run a specific test class (make test-class CLASS=io.cascadestore.lsm.CascadeStoreTest)"
	@echo "  test-method      - Run a specific test method (make test-method CLASS=io.cascadestore.lsm.CascadeStoreTest METHOD=testPutAndGet)"
	@echo "  help             - Show this help message"
