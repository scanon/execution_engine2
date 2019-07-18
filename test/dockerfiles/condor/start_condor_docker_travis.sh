#!/bin/bash

# TODO Pull this variable from a config
export docker_tag=kbase/ee2:condor_test_instance
docker pull ${docker_tag}
docker rm condor_test_container --force
cnt_id=`docker run -i -d  --privileged=true -v /sys/fs/cgroup:/sys/fs/cgroup:ro  --name condor_test_container  -p 9618:9618 ${docker_tag} /usr/sbin/init`
echo "Container id is $cnt_id"
docker ps
sleep 3
docker exec -i -u 0 ${cnt_id} ./start_condor.sh


