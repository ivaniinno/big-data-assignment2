#!/bin/bash

# Fail the entire script if something is failed
set -euo pipefail

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

# Put the parquet file into HDFS, i.parquet in my case
hdfs dfs -put -f data/i.parquet /
# Prepare plain text documents
spark-submit prepare_data.py

echo "Putting data into HDFS"
# Remove previous text files to avoid duplications
hdfs dfs -rm -r -f /data || true
# Put new text files into /data folder of HDFS
hdfs dfs -mkdir -p /data
hdfs dfs -put -f data/*.txt /data/

# Create dataset as one partition in /input/data folder
hdfs dfs -mkdir -p /input
hdfs dfs -rm -r -f /input/data || true
spark-submit prepare_data.py build-input

hdfs dfs -ls /data
hdfs dfs -ls /input/data
echo "Data preparation is done"
