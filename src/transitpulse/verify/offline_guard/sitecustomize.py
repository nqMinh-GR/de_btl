"""Offline socket guard for IT16 (fallback method of D-E26 when no network namespace is available).

This directory is put on ``PYTHONPATH`` of the processes started by the offline probe; Python imports
``sitecustomize`` at start-up. With ``TP_OFFLINE_GUARD=1`` every Python-level ``socket.connect`` /
``connect_ex`` to a non-loopback address and every DNS resolution of a non-loopback host name fails with
``ENETUNREACH`` / ``EAI_NONAME`` and is appended to ``TP_OFFLINE_GUARD_LOG`` (JSON lines).

Limits (reported by the probe): native sockets of librdkafka and of JVMs are not intercepted; JVM HTTP(S)
clients get a black-hole proxy through ``JAVA_TOOL_OPTIONS`` instead, and Kafka/Cassandra listen on loopback only.
"""

from __future__ import annotations

import errno
import ipaddress
import json
import os
import socket
import sys
import time
from typing import Any

_LOOPBACK_NAMES = frozenset({"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"})


def _host_text(host: Any) -> str | None:
    if host is None:
        return None
    if isinstance(host, bytes):
        host = host.decode("ascii", errors="replace")
    return str(host).strip().strip("[]").lower()


def _is_ip_literal(text: str) -> bool:
    try:
        ipaddress.ip_address(text.split("%", 1)[0])
        return True
    except ValueError:
        return False


def _allowed(host: Any) -> bool:
    text = _host_text(host)
    if text is None or text == "":
        return True
    if text in _LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(text.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _record(kind: str, target: Any) -> None:
    path = os.environ.get("TP_OFFLINE_GUARD_LOG")
    if not path:
        return
    line = {"ts": time.time(), "pid": os.getpid(), "kind": kind, "target": repr(target)[:200],
            "argv": " ".join(sys.argv[:4])[:300]}
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(line) + "\n")
    except OSError:
        pass


def _install() -> None:
    if os.environ.get("TP_OFFLINE_GUARD") != "1" or getattr(socket, "_tp_offline_guard", False):
        return
    orig_connect = socket.socket.connect
    orig_connect_ex = socket.socket.connect_ex
    orig_getaddrinfo = socket.getaddrinfo

    def _check(sock: socket.socket, address: Any) -> None:
        if getattr(sock, "family", None) not in (socket.AF_INET, socket.AF_INET6):
            return
        host = address[0] if isinstance(address, tuple) and address else address
        if not _allowed(host):
            _record("connect", address)
            raise OSError(errno.ENETUNREACH, f"TP_OFFLINE_GUARD blocked a non-loopback connection to {address!r}")

    def connect(self: socket.socket, address: Any) -> None:
        _check(self, address)
        return orig_connect(self, address)

    def connect_ex(self: socket.socket, address: Any) -> int:
        try:
            _check(self, address)
        except OSError:
            return errno.ENETUNREACH
        return orig_connect_ex(self, address)

    def getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
        text = _host_text(host)
        if text and not _allowed(host) and not _is_ip_literal(text):
            _record("resolve", host)
            raise socket.gaierror(socket.EAI_NONAME, f"TP_OFFLINE_GUARD blocked DNS resolution of {host!r}")
        return orig_getaddrinfo(host, *args, **kwargs)

    socket.socket.connect = connect  # type: ignore[method-assign]
    socket.socket.connect_ex = connect_ex  # type: ignore[method-assign]
    socket.getaddrinfo = getaddrinfo  # type: ignore[assignment]
    socket._tp_offline_guard = True  # type: ignore[attr-defined]


_install()
