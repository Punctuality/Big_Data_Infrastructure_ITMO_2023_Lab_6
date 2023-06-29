#!/bin/bash

cd ./contrib/ || exit 1

docker-compose up -d

cd ../

docker build --rm=true --build-arg HDFS_ADDRESS=${HDFS_NAMENODE_ADDRESS} -t spark-app-lab6 ./

if [ $? -ne 0 ]; then
  echo "Docker build failed"
  exit 1
else
  docker run --net contrib_default --link spark-master:spark-master spark-app-lab6
fi