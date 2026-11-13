#!/usr/bin/env python3
"""HTTP relay in front of ClickHouse for Experiment 025 failure injection.

`toudai-ingest` talks to ClickHouse over HTTP/1.1, so this relay parses each
request and response instead of relaying bytes blindly (Experiment 021's relay
spoke the native protocol). Every client connection gets its own upstream
connection, and requests on it are handled one at a time, as keep-alive HTTP/1.1
sends them.

Before forwarding a response to an INSERT, the relay reads the injection file
(`--control`, JSON). An empty or missing file means pass everything through.
Otherwise it holds one rule:

    {"match": "INSERT INTO canonical_events", "mode": "reset", "count": 1}

`match` is a substring of the query text from the URL's `query` parameter. The
first `count` matching INSERTs are injected, and the relay rewrites the file with
the count left. The INSERT itself always reaches ClickHouse in full, and the relay
reads ClickHouse's whole response before deciding, so an injected insert has
committed whenever that response was a 200 (the log records the status):

* `reset` closes the client connection with a TCP reset instead of sending the
  response. The client sees a network error at once and may retry.
* `withhold` sends nothing and keeps the connection open until the client gives
  up and closes it.

Harness-only code; nothing here is part of the tested design.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import socket
import struct
import threading
import urllib.parse

LOCK = threading.Lock()


def log(line: str) -> None:
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[relay] {stamp} {line}", flush=True)


class Conn:
    """Buffered reads from a socket."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buf = b""

    def fill(self) -> bool:
        data = self.sock.recv(65536)
        if not data:
            return False
        self.buf += data
        return True

    def read_until(self, marker: bytes) -> bytes | None:
        while marker not in self.buf:
            if not self.fill():
                return None
        at = self.buf.index(marker) + len(marker)
        out, self.buf = self.buf[:at], self.buf[at:]
        return out

    def read_exact(self, n: int) -> bytes | None:
        while len(self.buf) < n:
            if not self.fill():
                return None
        out, self.buf = self.buf[:n], self.buf[n:]
        return out


def headers_of(head: bytes) -> tuple[str, dict[str, str]]:
    lines = head.decode("latin-1").split("\r\n")
    fields = {}
    for line in lines[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            fields[name.strip().lower()] = value.strip()
    return lines[0], fields


def read_body(conn: Conn, fields: dict[str, str], until_close: bool) -> bytes | None:
    """The raw body bytes as framed on the wire (chunked framing kept)."""
    if "chunked" in fields.get("transfer-encoding", "").lower():
        out = b""
        while True:
            size_line = conn.read_until(b"\r\n")
            if size_line is None:
                return None
            out += size_line
            size = int(size_line.split(b";")[0].strip(), 16)
            if size == 0:
                # Trailer section, ended by an empty line.
                while True:
                    line = conn.read_until(b"\r\n")
                    if line is None:
                        return None
                    out += line
                    if line == b"\r\n":
                        return out
            chunk = conn.read_exact(size + 2)
            if chunk is None:
                return None
            out += chunk
    if "content-length" in fields:
        return conn.read_exact(int(fields["content-length"]))
    if until_close:
        while conn.fill():
            pass
        out, conn.buf = conn.buf, b""
        return out
    return b""


def take_rule(control: str, query: str) -> dict | None:
    with LOCK:
        try:
            with open(control) as f:
                text = f.read().strip()
        except FileNotFoundError:
            return None
        if not text:
            return None
        rule = json.loads(text)
        if rule.get("count", 0) <= 0 or rule["match"] not in query:
            return None
        rule["count"] -= 1
        tmp = control + ".tmp"
        with open(tmp, "w") as f:
            json.dump(rule, f)
        os.replace(tmp, control)
        return rule


def handle(client_sock: socket.socket, upstream_addr: tuple[str, int], control: str, n: int) -> None:
    client = Conn(client_sock)
    try:
        upstream_sock = socket.create_connection(upstream_addr)
    except OSError as error:
        log(f"conn {n}: upstream unreachable ({error})")
        client_sock.close()
        return
    upstream = Conn(upstream_sock)
    try:
        while True:
            head = client.read_until(b"\r\n\r\n")
            if head is None:
                return
            request_line, fields = headers_of(head)
            body = read_body(client, fields, until_close=False)
            if body is None:
                return
            target = request_line.split(" ")[1] if " " in request_line else ""
            params = urllib.parse.parse_qs(urllib.parse.urlsplit(target).query)
            query = params.get("query", [""])[0]
            upstream_sock.sendall(head + body)

            response_head = upstream.read_until(b"\r\n\r\n")
            if response_head is None:
                return
            status_line, response_fields = headers_of(response_head)
            response_body = read_body(upstream, response_fields, until_close=True)
            if response_body is None:
                return
            rule = take_rule(control, query) if query.lstrip().upper().startswith("INSERT") else None
            if rule is None:
                client_sock.sendall(response_head + response_body)
                continue
            status = status_line.split(" ")[1] if " " in status_line else "?"
            summary = " ".join(query.split()[:3])
            log(f"conn {n}: {rule['mode']} the response to {summary!r} (ClickHouse answered {status})")
            if rule["mode"] == "reset":
                client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                return
            if rule["mode"] == "withhold":
                while client.fill():
                    pass
                log(f"conn {n}: the client closed the withheld connection")
                return
            raise ValueError(f"unknown mode {rule['mode']!r}")
    except OSError as error:
        log(f"conn {n}: {error}")
    finally:
        client_sock.close()
        upstream_sock.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--upstream", type=int, required=True)
    parser.add_argument("--control", required=True)
    args = parser.parse_args()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", args.listen))
    server.listen(64)
    log(f"listening on 127.0.0.1:{args.listen}, relaying to 127.0.0.1:{args.upstream}")
    n = 0
    while True:
        sock, _ = server.accept()
        n += 1
        threading.Thread(
            target=handle, args=(sock, ("127.0.0.1", args.upstream), args.control, n), daemon=True
        ).start()


if __name__ == "__main__":
    raise SystemExit(main())
