#!/usr/bin/env python3

import re
import sys


TOKEN_RE = re.compile(r"[a-z0-9]+")


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    doc_length = len(TOKEN_RE.findall(text.lower()))

    print(f"DOC#{doc_id}\t{title}\t{doc_length}")
    print(f"__CORPUS__\t1\t{doc_length}")
