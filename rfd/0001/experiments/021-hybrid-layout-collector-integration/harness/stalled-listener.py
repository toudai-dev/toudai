#!/usr/bin/env python3
"""Stalled ClickHouse endpoint for Experiment 021 queue and crash phases.

Accepts native-protocol connections on a loopback port, drains bytes, and never
answers. The exporter's PrepareBatch therefore blocks until its own timeout,
which keeps the queue consumer busy while later requests accumulate in the
queue. Harness-only code; nothing here is part of the tested design.
"""

from __future__ import annotations

import argparse
import datetime
import socket
import threading


def log(line: str) -> None:
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[stall] {stamp} {line}", flush=True)


def handle(conn: socket.socket, addr, counter: int) -> None:
    log(f"connection {counter}: {addr[0]}:{addr[1]} accepted; reading without responding")
    try:
        while True:
            data = conn.recv(65536)
            if not data:
                break
    except OSError:
        pass
    finally:
        conn.close()
        log(f"connection {counter}: closed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen", type=int, required=True)
    args = parser.parse_args()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", args.listen))
    server.listen(16)
    log(f"listening on 127.0.0.1:{args.listen} (no responses)")
    counter = 0
    try:
        while True:
            conn, addr = server.accept()
            counter += 1
            threading.Thread(target=handle, args=(conn, addr, counter), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
