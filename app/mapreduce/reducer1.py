#!/usr/bin/env python3

import sys


current_term = None
term_counts = {}
doc_lengths = {}


def sort_key(doc_id):
    return int(doc_id) if doc_id.isdigit() else doc_id


def flush():
    global current_term, term_counts, doc_lengths

    if current_term is None:
        return

    doc_ids = sorted(term_counts, key=sort_key)
    print(f"VOCAB\t{current_term}\t{len(doc_ids)}")

    for doc_id in doc_ids:
        print(
            f"POSTING\t{current_term}\t{doc_id}\t{term_counts[doc_id]}\t{doc_lengths[doc_id]}"
        )

    term_counts = {}
    doc_lengths = {}


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 3:
        continue

    term, doc_id, doc_length = parts

    if current_term != term:
        flush()
        current_term = term

    term_counts[doc_id] = term_counts.get(doc_id, 0) + 1
    doc_lengths[doc_id] = int(doc_length)


flush()
