#!/usr/bin/env bash

docker network create --driver bridge hadoop_network

docker run -d  \
           -p 19999:19999 --name alluxio-master --hostname alluxio-master --network hadoop_network \
           -p 19998:19998 \
           -v $PWD/underStorage:/underStorage \
           -e ALLUXIO_MASTER_HOSTNAME=alluxio-master \
             -e ALLUXIO_UNDERFS_ADDRESS=hdfs://master:54310/alluxio \
             ovidanb/alluxio master

ALLUXIO_WORKER_CONTAINER_ID=$(docker run -d --shm-size=500MB \
             --network hadoop_network \
             --name alluxio-worker \
             -p 30000:30000 \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_MASTER_HOSTNAME=alluxio-master \
             -e ALLUXIO_WORKER_MEMORY_SIZE=500MB \
             -e ALLUXIO_UNDERFS_ADDRESS=hdfs://master:54310/alluxio \
             ovidanb/alluxio worker)