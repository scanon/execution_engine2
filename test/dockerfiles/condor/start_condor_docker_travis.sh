#!/bin/bash

docker build -f Dockerfile . -t condor_test_instance
docker rm  condor --force
cnt_id=`docker run -i -d  --privileged=true -v /sys/fs/cgroup:/sys/fs/cgroup:ro -v /Users/:/Users/ --name condor  -p 9618:9618 condor_test_instance /usr/sbin/init`
echo $cnt_id
sleep 1
docker exec -i -u 0 ${cnt_id} ./start_condor.sh

