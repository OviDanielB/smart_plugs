#!/usr/bin/env bash

./spark-submit --class Hello --master spark://spark-master:7077 --deploy-mode client \
hdfs://master:54310/app.jar hdfs://master:54310/dataset/d14_filtered.csv hdfs://master:54310/results