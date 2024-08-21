#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = volstock-project
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip
ACTIVATE_ENV := source venv/bin/activate

define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Create python interpreter environment.
create-environment:
	$(PYTHON_INTERPRETER) -m venv venv

# Execute python related functionalities from within the project's environment
requirements-ingest:
	$(call execute_in_env, $(PIP) install pg8000 -t ./deployment-packages/ingest-layer/python/ --upgrade)

requirements-process:
	$(call execute_in_env, $(PIP) install requests -t ./deployment-packages/process-layer/python/ --upgrade)
	$(call execute_in_env, $(PIP) install fastparquet -t ./deployment-packages/process-layer/python/ --upgrade)


requirements: create-environment requirements-ingest requirements-process

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install flake8
flake:
	$(call execute_in_env, $(PIP) install flake8)

pytest:
	$(call execute_in_env, $(PIP) install pytest moto boto3 pg8000 requests fastparquet)

coverage:
	$(call execute_in_env, $(PIP) install pytest-cov)


## Set up dev requirements (bandit, safety, flake)
dev-setup: bandit safety flake pytest coverage

# Build / Run

## Run the security test (bandit + safety)
security-test:
	$(call execute_in_env, $(PIP) freeze > ./requirements.txt)
	$(call execute_in_env, $(PIP) freeze --path ./deployment-packages/layer-ingest/python/ > ./requirements-ingest.txt)
	$(call execute_in_env, $(PIP) freeze --path ./deployment-packages/layer-process/python/ > ./requirements-process.txt)
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, safety check -r ./requirements-ingest.txt)
	$(call execute_in_env, safety check -r ./requirements-process.txt)
	$(call execute_in_env, bandit -lll ./src/ ./test/)


##Run the flake8 code styler
run-flake:
	$(call execute_in_env, flake8 --config=.flake8 ./src/ ./test)

## Run the unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest ./test -vv)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src ./test)

# Run all checks
run-checks: unit-test check-coverage
