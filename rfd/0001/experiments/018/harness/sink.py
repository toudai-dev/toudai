#!/usr/bin/env python3
"""Test sinks for Experiment 018.

capture    OTLP/HTTP endpoint that records what a downstream receives
           (records per request and the workspace attribute on each record)
tcp-stall  TCP listener that accepts connections and never responds, used to
           keep exporter consumers busy so queue behavior is observable
tcp-proxy  TCP proxy to the ClickHouse native port with explicit up/down modes,
           so a downstream outage can be injected without changing configs
"""

from __future__ import annotations

import argparse
import gzip
import json
import socket
import socketserver
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from opentelemetry.proto.collector.logs.v1 import logs_service_pb2  # noqa: E402
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2  # noqa: E402
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2  # noqa: E402

PARSERS = {
    "/v1/logs": (logs_service_pb2.ExportLogsServiceRequest, logs_service_pb2.ExportLogsServiceResponse),
    "/v1/traces": (trace_service_pb2.ExportTraceServiceRequest, trace_service_pb2.ExportTraceServiceResponse),
    "/v1/metrics": (metrics_service_pb2.ExportMetricsServiceRequest, metrics_service_pb2.ExportMetricsServiceResponse),
}


def describe_request(path, body):
    """Return a compact, JSON-safe description of an OTLP protobuf request."""
    request_cls, _ = PARSERS[path]
    req = request_cls()
    req.ParseFromString(body)
    out = {"path": path, "bytes": len(body), "resources": []}
    groups = getattr(req, "resource_logs", None) or getattr(req, "resource_spans", None) \
        or getattr(req, "resource_metrics", None)
    for group in groups:
        res_attrs = {kv.key: kv.value for kv in group.resource.attributes}
        workspace = res_attrs.get("lighthouse.workspace_id")
        workspace = workspace.string_value if workspace is not None else None
        scopes = getattr(group, "scope_logs", None) or getattr(group, "scope_spans", None) \
            or getattr(group, "scope_metrics", None)
        for scope_group in scopes:
            if path == "/v1/logs":
                items = list(scope_group.log_records)
            elif path == "/v1/traces":
                items = list(scope_group.spans)
            else:
                items = list(scope_group.metrics)
            out["resources"].append({
                "workspace": workspace,
                "scope": scope_group.scope.name,
                "records": len(items),
            })
    return out


class CaptureHandler(BaseHTTPRequestHandler):
    server_version = "lh018-capture"

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        if self.headers.get("Content-Encoding", "").lower() == "gzip":
            body = gzip.decompress(body)
        record = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "headers": {k.lower(): v for k, v in self.headers.items()},
        }
        try:
            record.update(describe_request(self.path, body))
        except Exception as exc:  # pragma: no cover - defensive for malformed input
            record["parse_error"] = repr(exc)
        self.server.records.append(record)
        if self.server.out_path:
            with open(self.server.out_path, "a") as handle:
                handle.write(json.dumps(record) + "\n")
        if self.server.fail_status:
            self.send_response(self.server.fail_status)
            self.end_headers()
            return
        if self.path in PARSERS:
            _, response_cls = PARSERS[self.path]
            payload = response_cls().SerializeToString()
            self.send_response(200)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):  # keep the console readable
        pass


class ReusableTCPServer(socketserver.ThreadingTCPServer):
    # Must be class attributes: TCPServer binds during __init__.
    allow_reuse_address = True
    daemon_threads = True


class StallHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # Accept and hold the connection open without ever responding, so the
        # exporter consumer occupies its timeout instead of failing fast.
        while True:
            time.sleep(1)


class ProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        mode = self.server.mode
        if mode == "reset":
            self.request.close()
            return
        try:
            upstream = socket.create_connection(self.server.target, timeout=5)
        except OSError:
            self.request.close()
            return

        def pump(src, dst):
            try:
                while True:
                    data = src.recv(65536)
                    if not data:
                        break
                    dst.sendall(data)
            except OSError:
                pass
            finally:
                try:
                    dst.shutdown(socket.SHUT_WR)
                except OSError:
                    pass

        t = threading.Thread(target=pump, args=(upstream, self.request), daemon=True)
        t.start()
        pump(self.request, upstream)
        upstream.close()
        self.request.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    capture = sub.add_parser("capture")
    capture.add_argument("--port", type=int, default=4320)
    capture.add_argument("--out", required=True)
    capture.add_argument("--fail-status", type=int, default=0,
                         help="if set, return this HTTP status instead of an OTLP success")

    stall = sub.add_parser("tcp-stall")
    stall.add_argument("--port", type=int, required=True)

    proxy = sub.add_parser("tcp-proxy")
    proxy.add_argument("--port", type=int, required=True)
    proxy.add_argument("--target", required=True)
    proxy.add_argument("--mode", choices=["up", "reset"], default="up")

    args = parser.parse_args(argv)

    if args.command == "capture":
        Path(args.out).touch()
        server = ThreadingHTTPServer(("127.0.0.1", args.port), CaptureHandler)
        server.records = []
        server.out_path = args.out
        server.fail_status = args.fail_status
        print(f"capture sink listening on 127.0.0.1:{args.port} -> {args.out}", flush=True)
        server.serve_forever()
    elif args.command == "tcp-stall":
        server = ReusableTCPServer(("127.0.0.1", args.port), StallHandler)
        print(f"stall listener on 127.0.0.1:{args.port}", flush=True)
        server.serve_forever()
    else:
        host, _, port = args.target.partition(":")
        server = ReusableTCPServer(("127.0.0.1", args.port), ProxyHandler)
        server.target = (host, int(port))
        server.mode = args.mode
        print(f"tcp proxy 127.0.0.1:{args.port} -> {args.target} mode={args.mode}", flush=True)
        server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
