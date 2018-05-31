#!/usr/bin/env bash
docker run -it --name mongo-client --network hadoop_network mongo:jessie /bin/bash -c 'mongo mongo-server:27017'
