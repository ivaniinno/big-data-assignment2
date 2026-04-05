#!/usr/bin/env python3

import math
import re
import sys
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


K1 = 1.0
B = 0.75
TOKEN_RE = re.compile(r"[a-z0-9]+")


query = " ".join(sys.argv[1:]).strip()
terms = sorted(set(TOKEN_RE.findall(query.lower())))

if not terms:
    print("No query terms provided.")
    sys.exit(0)

spark = SparkSession.builder.appName("search").getOrCreate()
sc = spark.sparkContext

cluster = Cluster(["cassandra-server"])
session = cluster.connect("search_engine")

stats = {}
for row in session.execute("SELECT name, value FROM corpus_stats"):
    stats[row.name] = row.value

document_count = stats.get("document_count", 0.0)
average_document_length = stats.get("average_document_length", 0.0)

idf_by_term = {}
postings = []

for term in terms:
    vocabulary_row = session.execute(
        "SELECT document_frequency FROM vocabulary WHERE term = %s",
        (term,),
    ).one()

    if vocabulary_row is None or vocabulary_row.document_frequency == 0:
        continue

    document_frequency = vocabulary_row.document_frequency
    idf_by_term[term] = math.log(document_count / document_frequency)

    for row in session.execute(
        "SELECT doc_id, term_frequency, doc_length FROM postings WHERE term = %s",
        (term,),
    ):
        postings.append((term, row.doc_id, row.term_frequency, row.doc_length))

if not postings:
    print("No matching documents found.")
    cluster.shutdown()
    spark.stop()
    sys.exit(0)

idf_broadcast = sc.broadcast(idf_by_term)
avgdl_broadcast = sc.broadcast(average_document_length)


def bm25(posting):
    term, doc_id, term_frequency, doc_length = posting
    idf = idf_broadcast.value[term]
    denominator = term_frequency + K1 * (1 - B + B * doc_length / avgdl_broadcast.value)
    score = idf * (((K1 + 1) * term_frequency) / denominator)
    return doc_id, score


top_documents = (
    sc.parallelize(postings)
    .map(bm25)
    .reduceByKey(lambda left, right: left + right)
    .takeOrdered(10, key=lambda row: (-row[1], row[0]))
)

print(f"Query: {query}")
print(f"Top {len(top_documents)} matching documents:")

for index, (doc_id, _) in enumerate(top_documents, start=1):
    row = session.execute(
        "SELECT title FROM documents WHERE doc_id = %s",
        (doc_id,),
    ).one()
    title = row.title.replace("_", " ") if row else ""
    print(f"{index}. {doc_id}\t{title}")

cluster.shutdown()
spark.stop()
