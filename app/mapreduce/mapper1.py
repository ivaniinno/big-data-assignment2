#!/usr/bin/env python3

import re
import sys


TOKEN_RE = re.compile(r"[a-z0-9]+")


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, _, text = parts
    tokens = TOKEN_RE.findall(text.lower())
    doc_length = len(tokens)

    for token in tokens:
        print(f"{token}\t{doc_id}\t{doc_length}")
