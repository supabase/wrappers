"""Deterministic anonymous HTTP object server for Zarr FDW integration tests."""

from __future__ import annotations

from collections import Counter
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import threading
import time
from urllib.parse import unquote, urlsplit


HOST = "0.0.0.0"
PORT = 8787
FIXTURE_ROOT = Path(os.environ.get("ZARR_HTTP_FIXTURE_ROOT", "/fixtures")).resolve()
FIXTURES = {"e2e.zarr", "e2e-v3.zarr", "e2e-ome-v3.zarr"}
MODES = {
    "anonymous_only",
    "bad_content_range",
    "deny_all",
    "mutate_shard",
    "no_etag",
    "oversize_metadata",
    "plain",
    "range_200",
    "redirect_chunk",
    "stall_chunk",
}
CASE_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
FORBIDDEN_HEADERS = ("Authorization", "Proxy-Authorization", "Cookie", "Referer")
OVERSIZE_METADATA_BYTES = 1024 * 1024 + 1


class CaseStats:
    def __init__(self) -> None:
        self.object_gets = 0
        self.redirect_sink_gets = 0
        self.proxy_sink_gets = 0
        self.forbidden_header_gets = 0
        self.paths: Counter[str] = Counter()
        self.ranges: Counter[str] = Counter()
        self.if_matches: Counter[str] = Counter()
        self.accept_encodings: Counter[str] = Counter()

    def as_dict(self) -> dict[str, object]:
        return {
            "object_gets": self.object_gets,
            "redirect_sink_gets": self.redirect_sink_gets,
            "proxy_sink_gets": self.proxy_sink_gets,
            "forbidden_header_gets": self.forbidden_header_gets,
            "paths": dict(sorted(self.paths.items())),
            "ranges": dict(sorted(self.ranges.items())),
            "if_matches": dict(sorted(self.if_matches.items())),
            "accept_encodings": dict(sorted(self.accept_encodings.items())),
        }


STATS: dict[str, CaseStats] = {}
STATS_LOCK = threading.Lock()


def increment(case: str, field: str) -> None:
    with STATS_LOCK:
        stats = STATS.setdefault(case, CaseStats())
        setattr(stats, field, getattr(stats, field) + 1)


def record_object_request(
    case: str,
    object_key: str,
    range_header: str | None,
    if_match: str | None,
    accept_encoding: str | None,
    forbidden: bool,
) -> None:
    with STATS_LOCK:
        stats = STATS.setdefault(case, CaseStats())
        stats.object_gets += 1
        stats.paths[object_key] += 1
        if range_header is not None:
            stats.ranges[range_header] += 1
        if if_match is not None:
            stats.if_matches[if_match] += 1
        stats.accept_encodings[accept_encoding or "<absent>"] += 1
        if forbidden:
            stats.forbidden_header_gets += 1


def decode_segments(path: str) -> list[str] | None:
    try:
        decoded = unquote(path, errors="strict")
    except UnicodeError:
        return None
    if "\x00" in decoded or "\\" in decoded:
        return None
    segments = [segment for segment in decoded.split("/") if segment]
    if any(segment in {".", ".."} for segment in segments):
        return None
    return segments


def fixture_object(fixture: str, object_segments: list[str]) -> tuple[Path, str] | None:
    if fixture not in FIXTURES or not object_segments:
        return None
    fixture_root = (FIXTURE_ROOT / fixture).resolve()
    candidate = fixture_root.joinpath(*object_segments).resolve(strict=False)
    try:
        candidate.relative_to(fixture_root)
    except ValueError:
        return None
    return candidate, "/".join(object_segments)


def parse_range(value: str, total: int) -> tuple[int, int] | None:
    if not value.startswith("bytes=") or "," in value:
        return None
    spec = value[6:]
    if spec.startswith("-"):
        length_text = spec[1:]
        if not length_text.isdigit():
            return None
        length = int(length_text)
        if length <= 0 or total <= 0:
            return None
        length = min(length, total)
        return total - length, total - 1
    if "-" not in spec:
        return None
    start_text, end_text = spec.split("-", 1)
    if not start_text.isdigit() or not end_text.isdigit():
        return None
    start = int(start_text)
    end = int(end_text)
    if start > end or start >= total or end >= total:
        return None
    return start, end


class ZarrHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def send_empty(self, status: int, **headers: str) -> None:
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name.replace("_", "-"), value)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def send_json(self, value: object) -> None:
        body = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        segments = decode_segments(parsed.path)
        if segments is None:
            self.send_empty(400)
            return
        if segments == ["__health"] and not parsed.query:
            self.send_empty(200)
            return
        if len(segments) == 2 and segments[0] == "__stats" and not parsed.query:
            case = segments[1]
            if not CASE_PATTERN.fullmatch(case):
                self.send_empty(400)
                return
            with STATS_LOCK:
                value = STATS.setdefault(case, CaseStats()).as_dict()
            self.send_json(value)
            return
        if len(segments) == 2 and segments[0] == "__sink" and not parsed.query:
            case = segments[1]
            if not CASE_PATTERN.fullmatch(case):
                self.send_empty(400)
                return
            increment(case, "redirect_sink_gets")
            self.send_empty(418)
            return
        if len(segments) == 2 and segments[0] == "__proxy" and not parsed.query:
            case = segments[1]
            if not CASE_PATTERN.fullmatch(case):
                self.send_empty(400)
                return
            increment(case, "proxy_sink_gets")
            self.send_empty(502)
            return
        if len(segments) < 5 or segments[0] != "stores" or parsed.query:
            self.send_empty(404)
            return

        case, mode, fixture = segments[1:4]
        if not CASE_PATTERN.fullmatch(case) or mode not in MODES:
            self.send_empty(404)
            return
        resolved = fixture_object(fixture, segments[4:])
        if resolved is None:
            self.send_empty(400)
            return
        path, object_key = resolved
        range_header = self.headers.get("Range")
        if_match = self.headers.get("If-Match")
        accept_encoding = self.headers.get("Accept-Encoding")
        forbidden = any(self.headers.get(name) is not None for name in FORBIDDEN_HEADERS)
        if mode == "anonymous_only" and accept_encoding != "identity":
            forbidden = True
        record_object_request(
            case,
            object_key,
            range_header,
            if_match,
            accept_encoding,
            forbidden,
        )

        if mode == "deny_all":
            self.send_empty(503)
            return
        if mode == "anonymous_only" and forbidden:
            self.send_empty(400)
            return
        if mode == "redirect_chunk" and object_key == "nested/raw/0.0.0":
            self.send_empty(
                302,
                Location=f"http://127.0.0.1:{PORT}/__sink/{case}",
            )
            return
        if mode == "oversize_metadata" and object_key == "nested/raw/.zarray":
            self.send_response(200)
            self.send_header("Content-Length", str(OVERSIZE_METADATA_BYTES))
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            return
        if not path.is_file():
            self.send_empty(404)
            return

        body = path.read_bytes()
        etag = f'"{sha256(body).hexdigest()}"'
        if mode == "stall_chunk" and object_key == "nested/raw/0.0.0":
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", etag)
            self.end_headers()
            self.wfile.flush()
            time.sleep(5)
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        if mode == "mutate_shard" and range_header is not None:
            if range_header.startswith("bytes=-"):
                etag = '"generation-a"'
            elif if_match == '"generation-a"':
                self.send_empty(412)
                return

        if if_match is not None and if_match != etag:
            self.send_empty(412)
            return
        if range_header is None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", etag)
            self.end_headers()
            self.wfile.write(body)
            return
        if mode == "range_200":
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", etag)
            self.end_headers()
            self.wfile.write(body)
            return

        selected = parse_range(range_header, len(body))
        if selected is None:
            self.send_empty(416, Content_Range=f"bytes */{len(body)}")
            return
        start, end = selected
        response_body = body[start : end + 1]
        content_range = f"bytes {start}-{end}/{len(body)}"
        if mode == "bad_content_range":
            content_range = f"bytes 0-{len(response_body) - 1}/{len(body)}"
        self.send_response(206)
        self.send_header("Content-Range", content_range)
        self.send_header("Content-Length", str(len(response_body)))
        if mode != "no_etag":
            self.send_header("ETag", etag)
        self.end_headers()
        self.wfile.write(response_body)


if __name__ == "__main__":
    if not FIXTURE_ROOT.is_dir():
        raise SystemExit(f"fixture root is unavailable: {FIXTURE_ROOT}")
    server = ZarrHttpServer((HOST, PORT), Handler)
    server.serve_forever()
