#!/bin/bash
SPARK=/usr/local/bigdata/spark/bin/spark-submit
rm lib.zip
zip lib.zip udf.py
nohup ${SPARK} --master yarn --num-executors 1 --driver-memory 1g --executor-memory 1g --executor-cores 1 --jars /usr/local/bigdata/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar \
     --queue default --py-files lib.zip main.py >./log/clean.log 2>./log/clean.log.wf &
