#!/bin/bash

docker build -f Dockerfile . -t condor_test_instance
docker rm  condor --force
cnt_id=`docker run -i -d  --privileged=true -v /sys/fs/cgroup:/sys/fs/cgroup:ro -v /Users/:/Users/ --name condor  -p 22:22 condor_test_instance /usr/sbin/init`
echo $cnt_id
docker exec -i -u 0 ${cnt_id} ./start_condor.sh

#ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@localhost

#New idea, start up a docker-compose, with two services, a python...
# Or actually just start up ssh inside here, and use it as a remote interpreter...
