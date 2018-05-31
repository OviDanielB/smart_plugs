#!/usr/bin/env bash

COM=$(kubectl apply -f deployment/alluxio-journal-volume.yaml)
if [ $? -ne 0 ]; then
    echo "Alluxio journaling volume creation failed"
    exit 1
fi

echo "Alluxio journaling volume creation succeeded"

COM=$(kubectl apply -f deployment/alluxio-master.yaml)
if [ $? -ne 0 ]; then
    echo "Alluxio master creation failed"
    exit 1
fi

echo "Alluxio master creation succeeded"


COM=$(kubectl apply -f deployment/alluxio-worker.yaml)
if [ $? -ne 0 ]; then
    echo "Alluxio worker creation failed"
    exit 1
fi

echo "Alluxio worker creation succeeded"
