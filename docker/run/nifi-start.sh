#!/usr/bin/env bash

HOSTNAME=nifi
HOST=localhost
PORT=9999
HDFS_DEFAULT_FS=hdfs://master:54310
JAR=1.1.32/nifi-deploy-config-1.1.32.jar

docker network create --driver bridge hadoop_network
docker run -t -i -p $PORT:$PORT -e NIFI_WEB_HTTP_PORT="$PORT" -e HDFS_DEFAULTS_FS="$HDFS_DEFAULT_FS" --network=hadoop_network -d --hostname=$HOSTNAME --name=nifi zanna94/nifi:latest

docker exec nifi bash -c "cd / && curl -O https://raw.githubusercontent.com/trillaura/smart_plugs/master/docker/build/NiFi/template.xml"

printf 'waiting for NiFi...\n'
until $(curl --output /dev/null --silent --head --fail http://$HOST:$PORT); do
    printf '.'
    sleep 5
done

docker exec -it nifi java -jar /nifi-deploy-config-1.1.32.jar -nifi http://$HOSTNAME:$PORT/nifi-api -conf /template2.0.xml -m deployTemplate





