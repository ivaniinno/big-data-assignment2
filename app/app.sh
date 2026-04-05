#!/bin/bash

set -euo pipefail

# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
rm -rf .venv
echo "Create virtual environment"
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
echo "Install packages"
python -m pip install --upgrade pip setuptools wheel -q
pip install -r requirements.txt  

# Package the virtual env.
rm -f .venv.tar.gz
venv-pack -o .venv.tar.gz
hdfs dfs -mkdir -p /app
hdfs dfs -put -f .venv.tar.gz /app/.venv.tar.gz

# Collect data
echo "Preparing data"
bash prepare_data.sh

# Run the indexer
echo "Running indexer"
bash index.sh

# Run the ranker
echo "Running query: film"
bash search.sh "film"

echo "Running query: legend"
bash search.sh "legend"

echo "Automatic run finished. Cluster master will stay alive for manual checks."
tail -f /dev/null
