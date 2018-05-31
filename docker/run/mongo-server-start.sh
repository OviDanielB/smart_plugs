#!/usr/bin/bash
docker run -it --name mongo-server --network hadoop_network -p 27017:27017 mongo:jessie /usr/bin/mongod --smallfiles --bind_ip_all
