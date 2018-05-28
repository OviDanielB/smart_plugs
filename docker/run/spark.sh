#!/usr/bin/env bash

SPARK_DOCKER_IMAGE="ovidanb/spark:latest"
SPARK_DOCKER_NETWORK="hadoop_network"


START_COMMAND="start"
STOP_COMMAND="stop"

SPARK_MASTER_HOSTNAME="spark-master"
SPARK_MASTER_CONTAINER_NAME="spark-master"
SPARK_PORT=7077
SPARK_UI_PORT=8080
SPARK_SLAVE_CONTAINER_NAME_BASE="sslave"

USAGE_MESS=$(cat <<-EOF
  Usage: spark <start|stop>
  Starts or stops spark node on Docker container.
  Options:
    --slaves [num] : number of slaves to create or remove (defaults to 1 if not specified)
EOF
)

if [ "$1" == "" ]; then
  echo "$USAGE_MESS"
  exit 1
fi

NUM_SLAVES=1
SLAVES=""

if [ "$1" == $START_COMMAND ]; then

    shift
    if [ "$1" == "" ]; then
        echo "$USAGE_MESS"
        exit 1
    fi

    while [ "$1" != "" ]; do
        case $1 in
        --slaves )     shift
                       NUM_SLAVES=$1;;
   #     * )            echo "$USAGE_MESS"
   #                    exit 1
        esac
        shift
    done

    #docker network create --driver bridge spark_network


    for i in $(seq 1 ${NUM_SLAVES})
    do
        SLAVES="${SLAVES};${SPARK_SLAVE_CONTAINER_NAME_BASE}${i}"
        SLAVE_CREATE=$(docker run -i -t -d -p $((4039 + ${i})):4040 -p $((8080 + ${i})):8081 --network ${SPARK_DOCKER_NETWORK} --name ${SPARK_SLAVE_CONTAINER_NAME_BASE}${i} ${SPARK_DOCKER_IMAGE})

        if [ $? == 0 ]; then
            echo "Successfully created slave ${SPARK_SLAVE_CONTAINER_NAME_BASE}${i}"
        fi
    done



    docker run -i -t -d --name ${SPARK_MASTER_CONTAINER_NAME} -e SLAVES=${SLAVES} \
    -p ${SPARK_UI_PORT}:${SPARK_UI_PORT} --hostname ${SPARK_MASTER_CONTAINER_NAME} \
    -p ${SPARK_PORT}:${SPARK_PORT}  --network ${SPARK_DOCKER_NETWORK} \
     ${SPARK_DOCKER_IMAGE}

    if [ $? -ne 0 ]; then
        echo "Spark Master container creation error"
        exit 1
    fi
    echo "Spark Master started on port ${SPARK_PORT} with web UI on port ${SPARK_UI_PORT}"
    exit 0
fi

if [ "$1" == ${STOP_COMMAND} ]; then

    shift
    if [ "$1" == "" ]; then
        echo "$USAGE_MESS"
        exit 1
    fi

    while [ "$1" != "" ]; do
        case $1 in
            --slaves )     shift
                           NUM_SLAVES=$1;;
        esac
        shift
    done

    for j in $(seq 1 ${NUM_SLAVES})
    do
        RM_SLAVE=$(docker rm -f ${SPARK_SLAVE_CONTAINER_NAME_BASE}${j})
        if [ $? == 0 ]; then
            echo "Successfully deleted slave ${SPARK_SLAVE_CONTAINER_NAME_BASE}${j}"
        fi
    done

    MASTER_REM=$(docker rm -f ${SPARK_MASTER_CONTAINER_NAME})
    if [ $? -ne 0 ]; then
        echo "Spark Master container removal error"
        exit 1
    fi

    echo "Spark Master container successfully removed"
    exit 0
fi

echo "${USAGE_MESS}"

