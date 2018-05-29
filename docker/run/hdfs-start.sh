#!/usr/bin/env bash
docker network create --driver bridge hadoop_network

docker run -t -i -p 50075:50075 -d --network=hadoop_network --name=slave1 matnar/hadoop
docker run -t -i -p 50076:50075 -d --network=hadoop_network --name=slave2 matnar/hadoop
docker run -t -i -p 50077:50075 -d --network=hadoop_network --name=slave3 matnar/hadoop
CONT=$(docker run -t -i -p 50070:50070 -d --network=hadoop_network --name=master matnar/hadoop)

docker exec master /bin/bash -c \
"chmod 700 /usr/local/hadoop/etc/hadoop/hadoop-env.sh;
    /usr/local/hadoop/etc/hadoop/hadoop-env.sh;
    hdfs namenode -format;
    /usr/local/hadoop/sbin/start-dfs.sh"

FILENAME=d14_filtered.csv

docker cp ../../dataset/$FILENAME $CONT:/

docker exec master /bin/bash -c \
"hdfs dfs -mkdir /dataset;
    hdfs dfs -put $FILENAME /dataset/"

docker cp ../../target/scala-2.11/smart_plugs-assembly-0.1.jar ${CONT}:/app.jar

docker exec master /bin/bash -c "hdfs dfs -put /app.jar /app.jar"