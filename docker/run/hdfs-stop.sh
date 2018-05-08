#!/usr/bin/env bash

docker rm -f slave1
docker rm -f slave2
docker rm -f slave3
docker rm -f master

docker network rm hadoop_network