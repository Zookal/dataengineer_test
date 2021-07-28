# Variable Declaration
COVERAGE_FOLDER := retail_etl
KNOWN_TARGETS = cov_report
ARGS := $(filter-out $(KNOWN_TARGETS),$(MAKECMDGOALS))

# HTML Coverage Report
ifeq ($(ARGS), html)
	COV_REPORT_TYPE := --cov-report html
endif

# XML Coverage Report
ifeq ($(ARGS), xml)
	COV_REPORT_TYPE := --cov-report xml
endif

PYTHON=python3.9

# Check for Type Hint inconsistencies
.PHONY: typehint
typehint:
	mypy --ignore-missing-imports $(COVERAGE_FOLDER)

# Run all Test Suites under the tests folder
.PHONY: test
test:
	 PYTHONPATH=. pytest tests -v -s

# Format the code into black formatting
.PHONY: black
black:
	black -l 110 $(COVERAGE_FOLDER)

# Check for Lint errors
.PHONY: lint
lint:
	flake8 $(COVERAGE_FOLDER)

# Check for Security Vulnerabilities
.PHONY: scan_security
scan_security:
	bandit $(COVERAGE_FOLDER)

# Clean up local development's cache data
.PHONY: clean
clean:
	find . -type f -name "*.pyc" | xargs rm -fr
	find . -type d -name __pycache__ | xargs rm -fr
	find . -type d -name .mypy_cache | xargs rm -fr
	find . -type d -name .pytest_cache | xargs rm -fr

# Run all Pre-commit Checks
.PHONY: checklist
checklist: black lint scan_security test clean

# Check Coverage Report
.DEFAULT: ;: do nothing

.SUFFIXES:
.PHONY: cov_report
cov_report:
	PYTHONPATH=. pytest --cov $(COVERAGE_FOLDER) $(COV_REPORT_TYPE)
