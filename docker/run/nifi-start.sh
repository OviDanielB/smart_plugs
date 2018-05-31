#!/usr/bin/env bash

HOSTNAME=nifi
HOST=localhost
PORT=9999
JAR=1.1.32/nifi-deploy-config-1.1.32.jar

docker network create --driver bridge hadoop_network
docker run -t -i -p $PORT:$PORT -e NIFI_WEB_HTTP_PORT="$PORT" --network=hadoop_network -d --hostname=$HOSTNAME --name=nifi zanna94/nifi

docker exec nifi bash -c "cd / && curl -O https://raw.githubusercontent.com/trillaura/smart_plugs/master/docker/build/NiFi/template.xml"

printf 'waiting for NiFi...\n'
until $(curl --output /dev/null --silent --head --fail http://$HOST:$PORT); do
    printf '.'
    sleep 5
done

docker exec -it nifi java -jar /nifi-deploy-config-1.1.32.jar -nifi http://$HOSTNAME:$PORT/nifi-api -conf /template.xml -m deployTemplate





