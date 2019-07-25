#!/bin/bash
# This script downloads the runner and copies it into the /runner/JobRunner.tgz
# The entrypoint.sh copies this into the /condor/shared directory
set -x
runner_dir=/runner
mkdir -p ${runner_dir} && cd ${runner_dir} && rm -rf JobRunner
git clone https://github.com/kbase/JobRunner.git
cd JobRunner
sleep 5
python3 -m venv venv && . ./venv/bin/activate
sleep 5
pip install -r requirements.txt
tar -czvf ${runner_dir}/JobRunner.tgz $runner_dir/JobRunner
