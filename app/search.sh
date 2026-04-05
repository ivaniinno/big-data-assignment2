#!/bin/bash

set -euo pipefail

QUERY="$*"

if [[ -z "${QUERY}" ]]; then
    echo "Usage: bash search.sh \"query text\"" >&2
    exit 1
fi

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.archive=hdfs:///apps/spark/spark-jars.jar \
    --archives hdfs:///app/.venv.tar.gz#.venv \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
    --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
    query.py "$QUERY"
