#!/usr/bin/env python3
"""TCP event gateway for PyFlink socketTextStream (Flink connects as a client).

- Port 9999 (FLINK_PORT): Flink connects here and reads newline-delimited JSON.
- Port 9998 (INGEST_PORT): send-event.sh pushes one JSON line per connection.
"""

from __future__ import annotations

import os
import socket
import threading
from queue import Empty, Queue

FLINK_HOST = os.environ.get("FLINK_HOST", "0.0.0.0")
FLINK_PORT = int(os.environ.get("FLINK_PORT", "9999"))
INGEST_HOST = os.environ.get("INGEST_HOST", "0.0.0.0")
INGEST_PORT = int(os.environ.get("INGEST_PORT", "9998"))

_lock = threading.Lock()
_flink_socket: socket.socket | None = None
_pending: Queue[str] = Queue()


def _set_flink_socket(conn: socket.socket | None) -> None:
    global _flink_socket
    with _lock:
        _flink_socket = conn


def _flush_pending() -> None:
    with _lock:
        if _flink_socket is None:
            return
        while True:
            try:
                line = _pending.get_nowait()
            except Empty:
                return
            _flink_socket.sendall(f"{line}\n".encode())


def _enqueue_event(line: str) -> None:
    _pending.put(line)
    _flush_pending()


def _flink_server() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((FLINK_HOST, FLINK_PORT))
    server.listen(1)
    print(f"Flink stream listening on {FLINK_HOST}:{FLINK_PORT}", flush=True)

    while True:
        conn, addr = server.accept()
        print(f"Flink connected from {addr}", flush=True)
        _set_flink_socket(conn)
        _flush_pending()
        try:
            while True:
                if not conn.recv(1):
                    break
        except OSError:
            pass
        finally:
            print(f"Flink disconnected from {addr}", flush=True)
            with _lock:
                if _flink_socket is conn:
                    _set_flink_socket(None)
            conn.close()


def _handle_ingest(conn: socket.socket) -> None:
    try:
        chunks: list[bytes] = []
        while True:
            data = conn.recv(4096)
            if not data:
                break
            chunks.append(data)
        payload = b"".join(chunks).decode().strip()
        if payload:
            for line in payload.splitlines():
                line = line.strip()
                if line:
                    _enqueue_event(line)
    finally:
        conn.close()


def _ingest_server() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((INGEST_HOST, INGEST_PORT))
    server.listen(32)
    print(f"Event ingest listening on {INGEST_HOST}:{INGEST_PORT}", flush=True)

    while True:
        conn, addr = server.accept()
        threading.Thread(
            target=_handle_ingest, args=(conn,), daemon=True
        ).start()


def main() -> None:
    threading.Thread(target=_flink_server, daemon=True).start()
    _ingest_server()


if __name__ == "__main__":
    main()
