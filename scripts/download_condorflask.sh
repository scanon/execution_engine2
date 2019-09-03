#!/bin/bash
# This script downloads the runner and copies it into the /runner/JobRunner.tgz
# The entrypoint.sh copies this into the /condor/shared directory
set -x
condor_flask=/condor_flask
mkdir -p ${condor_flask} && cd ${condor_flask} && rm -rf condorflask
git clone --single-branch --branch master https://github.com/matyasselmeci/condorflask.git
cd condorflask
python3 -m venv condorflask && . condorflask/bin/activate
pip install -r requirements.txt
chown kbase . && chmod +x ./rungunicorn
su --preserve-environment kbase --command ". condorflask/bin/activate && ./rungunicorn"