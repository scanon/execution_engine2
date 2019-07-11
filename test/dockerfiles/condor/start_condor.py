#!/bin/bash

# cd into the directory where the script is so we can use relative paths
script_path= $(dirname $(readlink -f $0)) || $(dirname $(greadlink -f $0))


cd $script_path
ls


#docker build -f DockerfileForCondor . -t condor_test

#docker run -ti --privileged=true -v /sys/fs/cgroup:/sys/fs/cgroup:ro /usr/sbin/init