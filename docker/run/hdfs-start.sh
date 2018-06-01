#!/usr/bin/env bash
docker network create --driver bridge hadoop_network

NET_OPTS="--network hadoop_network"

docker run -t -i -p 50075:50075 -d $NET_OPTS --name=slave1  matnar/hadoop
docker run -t -i -p 50076:50075 -d $NET_OPTS --name=slave2  matnar/hadoop
docker run -t -i -p 50077:50075 -d $NET_OPTS --name=slave3  matnar/hadoop
CONT=$(docker run -t -i -p 50070:50070 -d $NET_OPTS --name=master  matnar/hadoop)

docker exec master /bin/bash -c \
"chmod 700 /usr/local/hadoop/etc/hadoop/hadoop-env.sh;
    /usr/local/hadoop/etc/hadoop/hadoop-env.sh;
    hdfs namenode -format;
    /usr/local/hadoop/sbin/start-dfs.sh"

FILENAME1=d14_filtered.csv
FILENAME2=d14_filtered.parquet
FILENAME3=d14_filtered.avro

docker cp ../../dataset/filtered/$FILENAME1 $CONT:/
docker cp ../../dataset/filtered/$FILENAME2 $CONT:/
docker cp ../../dataset/filtered/$FILENAME3 $CONT:/

docker exec master /bin/bash -c \
"hdfs dfs -mkdir /dataset;
    hdfs dfs -mkdir /dataset/filtered;
    hdfs dfs -put $FILENAME1 /dataset/filtered/;
    hdfs dfs -put $FILENAME2 /dataset/filtered/;
    hdfs dfs -put $FILENAME3 /dataset/filtered/"

docker cp ../../target/scala-2.11/smart_plugs-assembly-0.1.jar ${CONT}:/app.jar

docker exec master /bin/bash -c "hdfs dfs -put /app.jar /app.jar"