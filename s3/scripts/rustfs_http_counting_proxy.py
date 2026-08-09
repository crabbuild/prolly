#!/usr/bin/env python3
"""Body-blind HTTP reverse proxy for local RustFS qualification.

The metrics file records only method, status class, byte counts, elapsed time,
and RustFS request ID. It deliberately never records URLs, query strings,
headers, credentials, object keys, or request/response bodies.
"""

from __future__ import annotations

import argparse
import http.client
import http.server
import json
import signal
import threading
import time
import urllib.parse


HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def read_chunked(handler: http.server.BaseHTTPRequestHandler) -> bytes:
    chunks: list[bytes] = []
    while True:
        line = handler.rfile.readline()
        if not line:
            raise ConnectionError("request ended inside chunked body")
        size = int(line.split(b";", 1)[0].strip(), 16)
        if size == 0:
            while handler.rfile.readline().strip():
                pass
            break
        chunks.append(handler.rfile.read(size))
        if handler.rfile.read(2) != b"\r\n":
            raise ConnectionError("invalid chunk terminator")
    return b"".join(chunks)


def make_handler(target: urllib.parse.SplitResult, metrics_path: str):
    metrics_lock = threading.Lock()

    class CountingProxy(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format: str, *_args: object) -> None:
            return

        def _request_body(self) -> bytes:
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                return read_chunked(self)
            length = int(self.headers.get("Content-Length", "0"))
            return self.rfile.read(length) if length else b""

        def _record(
            self,
            *,
            status: int,
            request_bytes: int,
            response_bytes: int,
            request_id: str | None,
            elapsed_ms: float,
        ) -> None:
            record = {
                "method": self.command,
                "status": status,
                "status_class": status // 100,
                "request_bytes": request_bytes,
                "response_bytes": response_bytes,
                "request_id": request_id,
                "elapsed_ms": round(elapsed_ms, 3),
            }
            with metrics_lock, open(metrics_path, "a", encoding="utf-8") as metrics:
                metrics.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
                metrics.write("\n")
                metrics.flush()

        def _forward(self) -> None:
            started = time.monotonic()
            request_body = self._request_body()
            headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower() not in HOP_BY_HOP
            }
            headers["Content-Length"] = str(len(request_body))
            connection = http.client.HTTPConnection(
                target.hostname,
                target.port or 80,
                timeout=60,
            )
            try:
                connection.request(self.command, self.path, body=request_body, headers=headers)
                response = connection.getresponse()
                response_body = response.read()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in HOP_BY_HOP | {"content-length"}:
                        self.send_header(key, value)
                if self.command == "HEAD":
                    content_length = response.getheader("Content-Length", "0")
                else:
                    content_length = str(len(response_body))
                self.send_header("Content-Length", content_length)
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(response_body)
                self.wfile.flush()
                self.close_connection = True
                self._record(
                    status=response.status,
                    request_bytes=len(request_body),
                    response_bytes=len(response_body),
                    request_id=response.getheader("x-request-id"),
                    elapsed_ms=(time.monotonic() - started) * 1_000,
                )
            except Exception:
                self._record(
                    status=0,
                    request_bytes=len(request_body),
                    response_bytes=0,
                    request_id=None,
                    elapsed_ms=(time.monotonic() - started) * 1_000,
                )
                raise
            finally:
                connection.close()

        do_DELETE = _forward
        do_GET = _forward
        do_HEAD = _forward
        do_OPTIONS = _forward
        do_PATCH = _forward
        do_POST = _forward
        do_PUT = _forward

    return CountingProxy


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True)
    parser.add_argument("--metrics", required=True)
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    target = urllib.parse.urlsplit(args.target)
    if target.scheme != "http" or target.hostname not in {"127.0.0.1", "localhost"}:
        raise SystemExit("qualification proxy target must be loopback HTTP")
    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", args.port), make_handler(target, args.metrics)
    )
    server.daemon_threads = True

    def stop(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    print(f"PROXY_READY port={server.server_port}", flush=True)
    server.serve_forever(poll_interval=0.1)
    server.server_close()


if __name__ == "__main__":
    main()
