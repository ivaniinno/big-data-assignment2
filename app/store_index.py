from cassandra.cluster import Cluster
import subprocess
import time


KEYSPACE = "search_engine"


def count_rows(path):
    return sum(1 for _ in stream_hdfs(path))


def stream_hdfs(path):
    process = subprocess.Popen(
        ["hdfs", "dfs", "-cat", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    for line in process.stdout:
        yield line.rstrip("\n")

    _, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(stderr.strip())


def connect():
    last_error = None

    for _ in range(10):
        try:
            cluster = Cluster(["cassandra-server"])
            session = cluster.connect()
            return cluster, session
        except Exception as error:
            last_error = error
            time.sleep(5)

    raise last_error


cluster, session = connect()

session.execute(
    f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """
)
session.set_keyspace(KEYSPACE)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY,
        document_frequency int
    )
    """
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS documents (
        doc_id bigint PRIMARY KEY,
        title text,
        doc_length int
    )
    """
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS postings (
        term text,
        doc_id bigint,
        term_frequency int,
        doc_length int,
        PRIMARY KEY (term, doc_id)
    )
    """
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS corpus_stats (
        name text PRIMARY KEY,
        value double
    )
    """
)

session.execute("TRUNCATE vocabulary")
session.execute("TRUNCATE documents")
session.execute("TRUNCATE postings")
session.execute("TRUNCATE corpus_stats")

insert_vocabulary = session.prepare(
    "INSERT INTO vocabulary (term, document_frequency) VALUES (?, ?)"
)
insert_document = session.prepare(
    "INSERT INTO documents (doc_id, title, doc_length) VALUES (?, ?, ?)"
)
insert_posting = session.prepare(
    "INSERT INTO postings (term, doc_id, term_frequency, doc_length) VALUES (?, ?, ?, ?)"
)
insert_stat = session.prepare(
    "INSERT INTO corpus_stats (name, value) VALUES (?, ?)"
)

vocabulary_total = count_rows("/indexer/vocabulary/part-00000")
print(f"Storing vocabulary ({vocabulary_total} rows)...", flush=True)
for index, line in enumerate(stream_hdfs("/indexer/vocabulary/part-00000"), start=1):
    term, document_frequency = line.split("\t")
    session.execute(insert_vocabulary, (term, int(document_frequency)))
    if vocabulary_total and index % max(1, vocabulary_total // 10) == 0:
        print(f"Vocabulary progress: {index}/{vocabulary_total}", flush=True)

documents_total = count_rows("/indexer/documents/part-00000")
print(f"Storing documents ({documents_total} rows)...", flush=True)
for index, line in enumerate(stream_hdfs("/indexer/documents/part-00000"), start=1):
    doc_id, title, doc_length = line.split("\t", 2)
    session.execute(insert_document, (int(doc_id), title, int(doc_length)))
    if documents_total and index % max(1, documents_total // 10) == 0:
        print(f"Documents progress: {index}/{documents_total}", flush=True)

postings_total = count_rows("/indexer/index/part-00000")
print(f"Storing postings ({postings_total} rows)...", flush=True)
for index, line in enumerate(stream_hdfs("/indexer/index/part-00000"), start=1):
    term, doc_id, term_frequency, doc_length = line.split("\t")
    session.execute(
        insert_posting,
        (term, int(doc_id), int(term_frequency), int(doc_length)),
    )
    if postings_total and index % max(1, postings_total // 10) == 0:
        print(f"Postings progress: {index}/{postings_total}", flush=True)

stats_total = count_rows("/indexer/stats/part-00000")
print(f"Storing corpus stats ({stats_total} rows)...", flush=True)
for index, line in enumerate(stream_hdfs("/indexer/stats/part-00000"), start=1):
    name, value = line.split("\t")
    session.execute(insert_stat, (name, float(value)))
    if stats_total and index % max(1, stats_total // 10) == 0:
        print(f"Corpus stats progress: {index}/{stats_total}", flush=True)

cluster.shutdown()
