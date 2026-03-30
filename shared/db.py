"""
Shared database module for BookedUp agent system.
All agents communicate via the events table in a shared SQLite database.
"""

import sqlite3
import json
import time
import os

DB_PATH = "/Users/bookedup/bookedup/shared/bookedup.db"


def get_connection(db_path=None):
    path = db_path or DB_PATH
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(db_path=None):
    conn = get_connection(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            source TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}',
            consumed INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
        CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);
        CREATE INDEX IF NOT EXISTS idx_events_consumed ON events(consumed);
        CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);

        CREATE TABLE IF NOT EXISTS agent_status (
            agent_name TEXT PRIMARY KEY,
            last_heartbeat REAL,
            status TEXT DEFAULT 'stopped',
            last_error TEXT,
            pid INTEGER
        );
    """)
    conn.commit()
    conn.close()


def publish_event(source, event_type, payload=None, db_path=None):
    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO events (timestamp, source, event_type, payload) VALUES (?, ?, ?, ?)",
        (time.time(), source, event_type, json.dumps(payload or {}))
    )
    conn.commit()
    conn.close()


def consume_events(event_type, limit=100, db_path=None):
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT * FROM events WHERE event_type = ? AND consumed = 0 ORDER BY timestamp ASC LIMIT ?",
        (event_type, limit)
    ).fetchall()

    if rows:
        ids = [r["id"] for r in rows]
        placeholders = ",".join("?" * len(ids))
        conn.execute(f"UPDATE events SET consumed = 1 WHERE id IN ({placeholders})", ids)
        conn.commit()

    conn.close()
    return [dict(r) for r in rows]


def peek_events(event_type=None, source=None, limit=50, since=None, db_path=None):
    conn = get_connection(db_path)
    query = "SELECT * FROM events WHERE 1=1"
    params = []

    if event_type:
        query += " AND event_type = ?"
        params.append(event_type)
    if source:
        query += " AND source = ?"
        params.append(source)
    if since:
        query += " AND timestamp > ?"
        params.append(since)

    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_agent_status(agent_name, status="running", error=None, db_path=None):
    conn = get_connection(db_path)
    conn.execute(
        """INSERT INTO agent_status (agent_name, last_heartbeat, status, last_error, pid)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(agent_name) DO UPDATE SET
               last_heartbeat = excluded.last_heartbeat,
               status = excluded.status,
               last_error = excluded.last_error,
               pid = excluded.pid""",
        (agent_name, time.time(), status, error, os.getpid())
    )
    conn.commit()
    conn.close()


def get_all_agent_statuses(db_path=None):
    conn = get_connection(db_path)
    rows = conn.execute("SELECT * FROM agent_status ORDER BY agent_name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# Initialize the database on first import
init_db()
