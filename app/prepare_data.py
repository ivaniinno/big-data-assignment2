import os
import sys
from pathlib import Path

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName("data preparation") \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


def create_documents(N):
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    for txt_file in data_dir.glob("*.txt"):
        txt_file.unlink()

    df = spark.read.parquet("/i.parquet") \
        .select("id", "title", "text") \
        .where(F.col("id").isNotNull()) \
        .where(F.col("title").isNotNull()) \
        .where(F.col("text").isNotNull()) \
        .where(F.length(F.trim(F.col("text"))) > 0) \
        .limit(N)

    for row in df.toLocalIterator():
        filename = sanitize_filename(f"{row['id']}_{row['title']}").replace(" ", "_")
        filename = filename.encode("ascii", "ignore").decode() + ".txt"
        (data_dir / filename).write_text(row["text"], encoding="utf-8")


def build_input():
    def to_line(file_and_text):
        path, text = file_and_text
        name = os.path.basename(path)[:-4]
        doc_id, doc_title = name.split("_", 1)
        return doc_id, doc_title, " ".join(text.split())

    spark.sparkContext.wholeTextFiles("hdfs:///data/*.txt") \
        .map(to_line) \
        .sortBy(lambda row: int(row[0])) \
        .map(lambda row: "\t".join(row)) \
        .coalesce(1) \
        .saveAsTextFile("hdfs:///input/data")


N = 100
if len(sys.argv) > 1 and sys.argv[1] == "build-input":
    build_input()
else:
    create_documents(N)

spark.stop()
