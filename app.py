#!/usr/bin/env python3
"""suite-letta — MegaMind log-ingest bridge (delivery end of Gap 1A).

Receives Vector http-sink batches at POST /api/v1/ingest, redacts again
(defense in depth — mirrors itsloop.sense.redact, including IPs which the
Vector source config does not scrub), embeds with the same deterministic
256-dim hashing embedder as itsloop.sense.HashingEmbedder so pgvector cosine
search is compatible with the semantic detector, and inserts into
itsloop_db.log_events on honcho-postgres (pgvector).

Self-initializing: creates the extension/table/index if missing (never
touches Letta's megamind_db schema). Returns 500 on storage failure so
Vector's disk buffer retries and no event is lost.
"""
import hashlib
import json
import math
import os
import re
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg2
from psycopg2.extras import execute_values

DIM = 256
PORT = int(os.environ.get("PORT", "8100"))
DSN = dict(
    host=os.environ.get("PGHOST", "honcho-postgres"),
    port=int(os.environ.get("PGPORT", "5432")),
    dbname=os.environ.get("PGDATABASE", "itsloop_db"),
    user=os.environ.get("PGUSER", "postgres"),
    password=os.environ["PGPASSWORD"],
)

_REDACTIONS = [
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"), "<email>"),
    (re.compile(r"(?i)bearer\s+[a-z0-9_\-.=]+"), "Bearer <token>"),
    (re.compile(r"(?i)(api[_-]?key|token|secret|password)\s*[=:]\s*\S+"), r"\1=<redacted>"),
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b"), "<ip>"),
]


def redact(text: str) -> str:
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def embed(text: str) -> list:
    vec = [0.0] * DIM
    for token in re.findall(r"[a-z0-9_.-]+", text.lower()):
        h = int.from_bytes(hashlib.sha256(token.encode()).digest()[:8], "big")
        vec[h % DIM] += 1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def parse_events(body: str) -> list:
    """Accept a JSON array, a single JSON object, or NDJSON."""
    body = body.strip()
    if not body:
        return []
    try:
        data = json.loads(body)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        events = []
        for line in body.splitlines():
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        return events


def parse_ts(value):
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


_conn = None


def db():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(**DSN)
        _conn.autocommit = True
    return _conn


def init_schema():
    with db().cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS log_events (
                id bigserial PRIMARY KEY,
                ts timestamptz NOT NULL DEFAULT now(),
                container text NOT NULL DEFAULT 'unknown',
                severity text NOT NULL DEFAULT 'info',
                message text NOT NULL,
                embedding vector({DIM}) NOT NULL,
                ingested_at timestamptz NOT NULL DEFAULT now()
            )"""
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS log_events_embedding_hnsw "
            "ON log_events USING hnsw (embedding vector_cosine_ops)"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS log_events_ts ON log_events (ts)")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (self.address_string(), fmt % args))

    def _send(self, code: int, obj: dict):
        payload = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path.rstrip("/") == "/health".rstrip("/") or self.path == "/health":
            try:
                with db().cursor() as cur:
                    cur.execute("SELECT count(*) FROM log_events")
                    rows = cur.fetchone()[0]
                self._send(200, {"ok": True, "service": "suite-letta", "log_events": rows})
            except Exception as exc:
                self._send(503, {"ok": False, "error": str(exc)})
        else:
            self._send(404, {"detail": "Not Found"})

    def do_POST(self):
        if self.path != "/api/v1/ingest":
            self._send(404, {"detail": "Not Found"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode("utf-8", "replace")
            events = parse_events(body)
            rows = []
            for event in events:
                if not isinstance(event, dict):
                    continue
                message = redact(str(event.get("message", "")))
                rows.append((
                    parse_ts(event.get("timestamp")),
                    str(event.get("container") or event.get("container_name") or "unknown")[:255],
                    str(event.get("severity") or "info")[:32],
                    message,
                    "[" + ",".join("%.6f" % v for v in embed(message)) + "]",
                ))
            if rows:
                with db().cursor() as cur:
                    execute_values(
                        cur,
                        "INSERT INTO log_events (ts, container, severity, message, embedding) VALUES %s",
                        rows,
                        template="(COALESCE(%s, now()), %s, %s, %s, %s::vector)",
                    )
            self._send(200, {"ok": True, "ingested": len(rows)})
        except Exception as exc:
            # 500 -> Vector keeps the batch in its disk buffer and retries.
            self._send(500, {"ok": False, "error": str(exc)})


if __name__ == "__main__":
    init_schema()
    print(f"suite-letta ingest listening on :{PORT}, storing to {DSN['host']}/{DSN['dbname']}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
