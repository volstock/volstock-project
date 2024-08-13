#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = volstock-project
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}:${WD}/src
SHELL := /bin/bash
PROFILE = default
PIP:=pip


## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Install flake8
flake:
	$(call execute_in_env, $(PIP) install flake8)


## Set up dev requirements (bandit, safety, black, flake)
dev-setup: bandit safety black coverage flake

# Build / Run

## Run the security test (bandit + safety)
security-test:
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, bandit -lll ./src/*.py)
## $(call execute_in_env, bandit -lll ./src/*.py ./test/*.py)

## Run the black code check
run-black:
	$(call execute_in_env, black  ./src/*.py)
# $(call execute_in_env, black  ./src/*.py ./test/*.py)


##Run the flake8 code styler
# run-flake:
# 	$(call execute_in_env, flake8  ./src/*.py)
# $(call execute_in_env, flake8  ./src/*.py ./test/*.py)

## Run the unit tests
# unit-test:
# 	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v)

## Run the coverage check
# check-coverage:
# 	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src test/)

# Run all checks
# run-checks: run-black run-flake unit-test check-coverage
# run-checks: run-black run-flake 
run-checks: run-black 