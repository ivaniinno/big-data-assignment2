#!/usr/bin/env python3

import sys


current_key = None
current_title = ""
current_length = 0
document_count = 0
total_document_length = 0


def flush():
    global current_key, current_title, current_length, document_count, total_document_length

    if current_key is None:
        return

    if current_key == "__CORPUS__":
        average_document_length = 0.0
        if document_count:
            average_document_length = total_document_length / document_count

        print(f"STAT\tdocument_count\t{document_count}")
        print(f"STAT\ttotal_document_length\t{total_document_length}")
        print(f"STAT\taverage_document_length\t{average_document_length}")
    else:
        doc_id = current_key.split("#", 1)[1]
        print(f"DOC\t{doc_id}\t{current_title}\t{current_length}")


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t", 2)
    if len(parts) != 3:
        continue

    key, value1, value2 = parts

    if current_key != key:
        flush()
        current_key = key
        current_title = ""
        current_length = 0
        document_count = 0
        total_document_length = 0

    if key == "__CORPUS__":
        document_count += int(value1)
        total_document_length += int(value2)
    else:
        current_title = value1
        current_length = int(value2)


flush()
