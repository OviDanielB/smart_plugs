#!/usr/bin/env bash

START_COMMAND="start"
STOP_COMMAND="stop"

SPARK_MASTER_HOSTNAME="spark-master"
SPARK_MASTER_CONTAINER_NAME="spark"
SPARK_PORT=7077
SPARK_UI_PORT=8080

USAGE_MESS=$(cat <<-EOF
  Usage: spark <start|stop>
  Starts or stops spark node on Docker container.
EOF
)

if [ "$1" == "" ]; then
  echo "$USAGE_MESS"
  exit 1
fi

if [ "$1" == $START_COMMAND ]; then
    docker run -i -t -d --name ${SPARK_MASTER_CONTAINER_NAME} --hostname ${SPARK_MASTER_HOSTNAME} \
    -p ${SPARK_UI_PORT}:${SPARK_UI_PORT} \
    -p ${SPARK_PORT}:${SPARK_PORT} \
     ovidanb/spark:latest

    if [ $? -ne 0 ]; then
        echo "Spark Master container creation error"
        exit 1
    fi
    echo "Spark Master started on port ${SPARK_PORT} with web UI on port ${SPARK_UI_PORT}"
    exit 0
fi

if [ "$1" == ${STOP_COMMAND} ]; then
    docker rm -f ${SPARK_MASTER_CONTAINER_NAME}
    if [ $? -ne 0 ]; then
        echo "Spark Master container removal error"
        exit 1
    fi

    echo "Spark Master container successfully removed"
    exit 0
fi

echo "${USAGE_MESS}"

