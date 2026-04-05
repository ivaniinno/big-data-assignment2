#!/bin/bash

set -euo pipefail

INPUT_PATH="${1:-/input/data}"
STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib" -name "hadoop-streaming*.jar" | head -n 1)
TMP_DIR=$(mktemp -d)

cleanup() {
    rm -rf "$TMP_DIR"
}

trap cleanup EXIT

if [[ -z "${STREAMING_JAR}" ]]; then
    echo "Could not find Hadoop streaming jar" >&2
    exit 1
fi

run_job() {
    local mapper="$1"
    local reducer="$2"
    local output="$3"

    hdfs dfs -rm -r -f "$output" >/dev/null 2>&1 || true

    hadoop jar "$STREAMING_JAR" \
        -D mapreduce.job.reduces=1 \
        -files "mapreduce/${mapper},mapreduce/${reducer}" \
        -mapper "python3 ${mapper}" \
        -reducer "python3 ${reducer}" \
        -input "$INPUT_PATH" \
        -output "$output"
}

source .venv/bin/activate

hdfs dfs -rm -r -f /tmp/indexer >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /indexer >/dev/null 2>&1 || true

echo "Start index creation"
run_job mapper1.py reducer1.py /tmp/indexer/pipeline1
run_job mapper2.py reducer2.py /tmp/indexer/pipeline2

hdfs dfs -cat /tmp/indexer/pipeline1/part-* | awk -F '\t' '$1=="POSTING"{print $2 "\t" $3 "\t" $4 "\t" $5}' > "$TMP_DIR/index.tsv"
hdfs dfs -cat /tmp/indexer/pipeline1/part-* | awk -F '\t' '$1=="VOCAB"{print $2 "\t" $3}' > "$TMP_DIR/vocabulary.tsv"
hdfs dfs -cat /tmp/indexer/pipeline2/part-* | awk -F '\t' '$1=="DOC"{print $2 "\t" $3 "\t" $4}' > "$TMP_DIR/documents.tsv"
hdfs dfs -cat /tmp/indexer/pipeline2/part-* | awk -F '\t' '$1=="STAT"{print $2 "\t" $3}' > "$TMP_DIR/stats.tsv"

hdfs dfs -mkdir -p /indexer/index /indexer/vocabulary /indexer/documents /indexer/stats
hdfs dfs -put -f "$TMP_DIR/index.tsv" /indexer/index/part-00000
hdfs dfs -put -f "$TMP_DIR/vocabulary.tsv" /indexer/vocabulary/part-00000
hdfs dfs -put -f "$TMP_DIR/documents.tsv" /indexer/documents/part-00000
hdfs dfs -put -f "$TMP_DIR/stats.tsv" /indexer/stats/part-00000

hdfs dfs -ls /indexer
hdfs dfs -ls /indexer/index
hdfs dfs -ls /indexer/vocabulary
hdfs dfs -ls /indexer/documents
hdfs dfs -ls /indexer/stats
echo "Index creation is done"
