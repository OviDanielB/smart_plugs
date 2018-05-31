#!/usr/bin/env bash

CM=$(kubectl create configmap alluxio-config --from-file=ALLUXIO_CONFIG=conf/alluxio.properties)

if [ $? -ne 0 ]; then
    echo "Error in creating config map. Maybe already exists"
    exit 1
fi

echo "Config map created successfully"