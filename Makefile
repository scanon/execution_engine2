SERVICE = execution_engine2
SERVICE_CAPS = execution_engine2
SPEC_FILE = execution_engine2.spec
URL = https://kbase.us/services/execution_engine2
DIR = $(shell pwd)
LIB_DIR = lib
SCRIPTS_DIR = scripts
TEST_DIR = test
LBIN_DIR = bin
WORK_DIR = /kb/module/work/tmp
EXECUTABLE_SCRIPT_NAME = run_$(SERVICE_CAPS)_async_job.sh
STARTUP_SCRIPT_NAME = start_server.sh
TEST_SCRIPT_NAME = run_tests.sh
CONDOR_DOCKER_IMAGE_TAG_NAME = kbase/ee2:condor_test_instance

.PHONY: test

default: compile

all: compile build build-startup-script build-executable-script build-test-script

compile:
	kb-sdk compile $(SPEC_FILE) \
		--out $(LIB_DIR) \
		--pysrvname $(SERVICE_CAPS).$(SERVICE_CAPS)Server \
		--pyimplname $(SERVICE_CAPS).$(SERVICE_CAPS)Impl;

build:
	chmod +x $(SCRIPTS_DIR)/entrypoint.sh

build-executable-script:
	mkdir -p $(LBIN_DIR)
	echo '#!/bin/bash' > $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)
	echo 'script_dir=$$(dirname "$$(readlink -f "$$0")")' >> $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)
	echo 'export PYTHONPATH=$$script_dir/../$(LIB_DIR):$$PATH:$$PYTHONPATH' >> $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)
	echo 'python -u $$script_dir/../$(LIB_DIR)/$(SERVICE_CAPS)/$(SERVICE_CAPS)Server.py $$1 $$2 $$3' >> $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)
	chmod +x $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)

build-startup-script:
	mkdir -p $(LBIN_DIR)
	echo '#!/bin/bash' > $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	echo 'script_dir=$$(dirname "$$(readlink -f "$$0")")' >> $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	echo 'export KB_DEPLOYMENT_CONFIG=$$script_dir/../deploy.cfg' >> $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	echo 'export PYTHONPATH=$$script_dir/../$(LIB_DIR):$$PATH:$$PYTHONPATH' >> $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	#Fix this
	echo 'uwsgi --master --processes 5 --threads 5 --http :5000 --uid 1000 --wsgi-file $$script_dir/../$(LIB_DIR)/$(SERVICE_CAPS)/$(SERVICE_CAPS)Server.py' >> $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	chmod +x $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)

build-test-script:
	echo '#!/bin/bash' > $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'script_dir=$$(dirname "$$(readlink -f "$$0")")' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'export KB_DEPLOYMENT_CONFIG=$$script_dir/../deploy.cfg' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'export KB_AUTH_TOKEN=`cat /kb/module/work/token`' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'echo "Removing temp files..."' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'rm -rf $(WORK_DIR)/*' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'echo "...done removing temp files."' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'export PYTHONPATH=$$script_dir/../$(LIB_DIR):$$PATH:$$PYTHONPATH' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'cd $$script_dir/../$(TEST_DIR)' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'python -m nose --with-coverage --cover-package=$(SERVICE_CAPS) --cover-html --cover-html-dir=/kb/module/work/test_coverage --nocapture --nologcapture .' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	chmod +x $(TEST_DIR)/$(TEST_SCRIPT_NAME)

TEST_FILES = test/ee2_scheduler_test.py test/ee2_SDKMethodRunner_test.py test/ee2_MongoUtil_test.py test/ee2_models_test.py test/ee2_server_test.py test/ee2_authutil_test.py test/ee2_workspaceauth_test.py test/ee2_authstrategy_test.py

setup-database:
	# Set up travis user in mongo
	nosetests -x -v --nocapture --nologcapture test/ee2_check_configure_mongo_docker.py

test:
	# Requires htcondor python bindings
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html --cover-package=execution_engine2 test/ee2_scheduler_test.py
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html --cover-package=execution_engine2 test/ee2_SDKMethodRunner_test.py
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html --cover-package=execution_engine2 test/ee2_MongoUtil_test.py
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html --cover-package=execution_engine2 test/ee2_server_test.py

test-models:
	# Requires travis user to be set up
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html --cover-package=execution_engine2 test/ee2_models_test.py

test-coverage:
	# Assumes setup-database run in previous step
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html --cover-package=execution_engine2 $(TEST_FILES)

#test-in-docker:
#    docker-compose up -d -f test/dockerfiles/condor/docker-compose.yml
#    docker-compose run -f



test-with-docker:
	# For Local Testing
	# Test with docker-compose versions of condor and mongo
	./test/dockerfiles/condor/start_condor_mongo_for_tests.sh
	# Set up travis user in mongo
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html --cover-package=execution_engine2 ee2_check_configure_mongo_docker
	# Run tests using python installed in travis, but with mongo and condor running in docker containers
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html --cover-package=execution_engine2 test/ee2_scheduler_test.py
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html --cover-package=execution_engine2 ee2_models_test


integration_test:
	./test/dockerfiles/condor/start_condor_docker_travis.sh
	nosetests -x -v --nocapture --nologcapture --with-coverage --cover-html test/ee2_scheduler_integration_test.py

clean:
	rm -rfv $(LBIN_DIR)

build-docker-image:
	./build/build_docker_image.sh

build-condor-test-image:
	cd test/dockerfiles/condor && echo `pwd` && docker build -f Dockerfile . -t $(CONDOR_DOCKER_IMAGE_TAG_NAME)
	docker push $(CONDOR_DOCKER_IMAGE_TAG_NAME)
