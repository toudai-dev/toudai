#!/usr/bin/env python3
"""Run statements against the disposable ClickHouse HTTP endpoint.

The ClickHouse 26.8 HTTP interface rejects multiple statements per request (no
``multiquery`` setting in this build), so the harness submits one statement at a
time. ``{{DB}}`` in the SQL text is replaced with the target database name, which
lets the DDL file stay database-agnostic.

Usage:
  sql.py --db lh021 --file serving-tables.sql
  sql.py --db lh021 --sql "SELECT 1"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests

ENDPOINT = "http://127.0.0.1:18224"


def statements(text: str) -> list[str]:
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        # Trailing comments after a statement are not used in this harness.
        lines.append(line)
    joined = "\n".join(lines)
    return [part.strip() for part in joined.split(";") if part.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--file")
    source.add_argument("--sql")
    args = parser.parse_args()

    text = Path(args.file).read_text(encoding="utf-8") if args.file else args.sql
    text = text.replace("{{DB}}", args.db)

    for statement in statements(text):
        resp = requests.post(ENDPOINT, params={"database": args.db}, data=statement.encode(),
                             timeout=120)
        if resp.status_code != 200:
            print(f"statement failed: {statement[:200]}\n{resp.text[:500]}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
