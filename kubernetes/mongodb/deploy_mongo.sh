#!/usr/bin/env bash

V=$(kubectl apply -f deployments/mongo_persistent_volume_claim.yaml)
if [ $? -ne 0 ]; then
    echo "Mongo persistent volume claim failed"
    exit 1
fi

echo "Mongo persistent volume claim succeeded"


V=$(kubectl apply -f deployments/mongo_service.yaml)
if [ $? -ne 0 ]; then
    echo "Mongo service deployment failed"
    exit 1
fi

echo "Mongo service deployment succeeded"