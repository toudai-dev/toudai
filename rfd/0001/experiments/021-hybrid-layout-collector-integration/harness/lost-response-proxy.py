#!/usr/bin/env python3
"""Bounded TCP relay for Experiment 021 failure injection.

Two uses:

* ``--drop-insert-responses 0`` forwards normally. The crash phase uses this to
  put a live ClickHouse behind the port the queued batches were admitted against.
* ``--drop-insert-responses N`` withholds the server's response to a ClickHouse
  native-protocol INSERT so the client sees a failure for a write that committed.
  That is the ambiguous insert: the destination committed, the client timed out,
  and the exporter helper retried a write that had already happened.
* ``--match SUBSTRING`` restricts the withholding to an INSERT statement whose
  text contains ``SUBSTRING`` (for example ``serving_logs``), so the injection can
  be aimed at the second of the two ordered inserts. Without it the first INSERT
  statement is claimed.

The earlier revision of this relay closed both sockets as soon as the first
server bytes arrived after an INSERT. That was measured to abort the insert
instead of committing it: ClickHouse's first bytes are the sample block the
client needs before it streams its data, so the relay cut the connection while
the server was still waiting for that data (query_log recorded
``ExceptionWhileProcessing``, code 32). The relay now keeps the connection open
and only stops forwarding bytes from the server once the insert's data has
already crossed the relay, so the server finishes and commits while the client
never receives a response.

Withholding (not closing) is what makes the outcome deterministic: the client
stays parked until its own request timeout, the server keeps the write, and the
retry then writes the same event a second time. The grace period only decides how
long the relay waits for client silence after the insert data before it stops
forwarding the response; the insert is already committed when the relay sees the
server's result.

This is harness-only code: it exists to create a specific failure inside the orb
and is not a component of the tested design.
"""

from __future__ import annotations

import argparse
import datetime
import socket
import threading
import time

INSERT_MARKER = b"INSERT INTO"


def log(line: str) -> None:
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[proxy] {stamp} {line}", flush=True)


class Injection:
    def __init__(self, drops: int, grace_ms: int, match: str | None = None):
        self.drops_left = drops
        self.grace = grace_ms / 1000.0
        self.match = match
        self.lock = threading.Lock()
        self.dropped = 0

    def armed(self) -> bool:
        """Whether the injection still has a response to withhold (diagnostics)."""
        with self.lock:
            return self.drops_left > 0

    def note_client_chunk(self, data: bytes) -> None:
        if self.drops_left <= 0:
            return
        if INSERT_MARKER in data:
            log(f"saw INSERT statement ({len(data)} bytes)")

    def targets(self, data: bytes) -> bool:
        """Whether this INSERT statement is the injection target."""
        if self.match is None:
            return True
        return self.match.encode() in data

    def claim(self) -> bool:
        """Claim the single withheld response at INSERT time.

        Claiming when the statement is seen (rather than when the connection is
        accepted) keeps the injection to exactly one insert even though the
        native client keeps a pool of idle connections that were all opened
        before the insert.
        """
        with self.lock:
            if self.drops_left <= 0:
                return False
            self.drops_left -= 1
            self.dropped += 1
            return True


def relay(src: socket.socket, dst: socket.socket, chunk_hook=None, withhold_hook=None) -> None:
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            if chunk_hook is not None:
                chunk_hook(data)
            if withhold_hook is not None and withhold_hook(data):
                # Stop forwarding in this direction but keep the socket open so
                # the peer keeps working; the client is left waiting on a
                # response that will never arrive.
                continue
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for sock in (src, dst):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def handle(conn: socket.socket, addr, target, injection: Injection, counter: int) -> None:
    host, port = target
    try:
        upstream = socket.create_connection((host, port), timeout=10)
    except OSError as err:
        log(f"connection {counter}: upstream connect failed: {err}")
        conn.close()
        return
    upstream.settimeout(None)
    conn.settimeout(None)
    log(f"connection {counter}: {addr[0]}:{addr[1]} -> {host}:{port}")

    # The injection is claimed when the first INSERT statement is seen on any
    # connection, so exactly one client request is affected. The withheld packet
    # is the first server payload that follows the client's own insert data: the
    # statement chunk is answered with the sample block, then the client streams
    # its data, and only then does the server send the result that must not reach
    # the client.
    state = {
        "withhold_response": False,
        "client_data_after_claim": False,
        "withholding": False,
        "last_client_at": time.monotonic(),
    }

    def client_chunk(data: bytes) -> None:
        state["last_client_at"] = time.monotonic()
        injection.note_client_chunk(data)
        if state["withhold_response"]:
            state["client_data_after_claim"] = True
        elif INSERT_MARKER in data and injection.targets(data) and injection.claim():
            state["withhold_response"] = True
            log(f"connection {counter}: claimed the INSERT response for withholding")

    def server_chunk(_data: bytes) -> bool:
        if state["withholding"]:
            return True
        if not state["withhold_response"] or not state["client_data_after_claim"]:
            return False
        # The grace period only makes sure the server-side commit completed before
        # the client is left hanging on its own timeout.
        if injection.grace > 0:
            time.sleep(injection.grace)
        state["withholding"] = True
        log(f"connection {counter}: withholding the INSERT response "
            f"(the server holds the committed write); the client will time out")
        return True

    t1 = threading.Thread(target=relay, args=(conn, upstream, client_chunk, None), daemon=True)
    t2 = threading.Thread(target=relay, args=(upstream, conn, None, server_chunk), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    log(f"connection {counter}: closed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--target", required=True, help="host:port")
    parser.add_argument("--drop-insert-responses", type=int, default=0)
    parser.add_argument("--grace-ms", type=int, default=300)
    parser.add_argument("--match", default=None,
                        help="only withhold an INSERT statement containing this substring")
    args = parser.parse_args()

    host, _, port = args.target.partition(":")
    target = (host, int(port))
    injection = Injection(args.drop_insert_responses, args.grace_ms, args.match)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", args.listen))
    server.listen(16)
    log(f"listening on 127.0.0.1:{args.listen} -> {host}:{port} "
        f"(drop_insert_responses={args.drop_insert_responses}, match={args.match})")

    counter = 0
    try:
        while True:
            conn, addr = server.accept()
            counter += 1
            threading.Thread(target=handle, args=(conn, addr, target, injection, counter),
                             daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        log(f"stopped after {counter} connections, {injection.dropped} withheld INSERT responses")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
