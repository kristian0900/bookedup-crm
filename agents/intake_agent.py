"""
Intake Agent — Ace Demo Centerpiece
Reads incoming messages from the centralized Telegram poller (via events table).
When a customer sends a /job command or freeform work request, creates a work order
in the database and notifies JR.

Supported intake formats (via Telegram):
  /job <customer> | <address> | <description>
  /job John Smith | 123 Main St Mesa AZ | AC not cooling

Also accepts freeform messages and attempts to parse them as work requests.
"""

import sys
import os
import time
import signal
import json
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import (
    update_agent_status, publish_event, consume_events, get_connection, init_db
)
from shared.notify import send_telegram, send_alert
from shared.logger import get_logger

AGENT_NAME = "intake_agent"
INTERVAL = 5

log = get_logger(AGENT_NAME)
running = True


def shutdown(signum, frame):
    global running
    log.info("Shutting down intake agent")
    update_agent_status(AGENT_NAME, status="stopped")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def init_work_orders_table():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS work_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at REAL NOT NULL,
            customer_name TEXT NOT NULL,
            address TEXT,
            description TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            priority TEXT DEFAULT 'normal',
            assigned_to TEXT DEFAULT 'JR',
            notes TEXT DEFAULT '',
            updated_at REAL,
            completed_at REAL,
            telegram_msg_id INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_wo_status ON work_orders(status);
        CREATE INDEX IF NOT EXISTS idx_wo_created ON work_orders(created_at);
    """)
    conn.commit()
    conn.close()


def parse_job_command(text):
    match = re.match(r"^/job\s+(.+)", text, re.IGNORECASE)
    if not match:
        return None
    parts = [p.strip() for p in match.group(1).split("|")]
    if len(parts) >= 3:
        return {"customer_name": parts[0], "address": parts[1], "description": " | ".join(parts[2:])}
    elif len(parts) == 2:
        return {"customer_name": parts[0], "address": "", "description": parts[1]}
    elif len(parts) == 1:
        return {"customer_name": "Unknown", "address": "", "description": parts[0]}
    return None


def parse_freeform(text):
    if text.startswith("/"):
        return None
    if len(text) < 10:
        return None
    keywords = [
        "ac ", "a/c", "hvac", "heat", "cool", "furnace", "thermostat",
        "leak", "plumb", "drain", "water heater", "pipe",
        "electric", "outlet", "breaker", "wire", "panel",
        "repair", "fix", "broken", "not working", "install",
    ]
    if not any(kw in text.lower() for kw in keywords):
        return None
    return {"customer_name": "Unknown (freeform)", "address": "", "description": text}


def already_processed(msg_id):
    """Check if a work order already exists for this Telegram message."""
    if not msg_id:
        return False
    conn = get_connection()
    row = conn.execute(
        "SELECT id FROM work_orders WHERE telegram_msg_id = ?", (msg_id,)
    ).fetchone()
    conn.close()
    return row is not None


def create_work_order(job_data, msg_id=None):
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO work_orders
           (created_at, customer_name, address, description, status, telegram_msg_id)
           VALUES (?, ?, ?, ?, 'new', ?)""",
        (time.time(), job_data["customer_name"], job_data.get("address", ""),
         job_data["description"], msg_id)
    )
    wo_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return wo_id


def main():
    init_db()
    init_work_orders_table()
    log.info("Intake agent starting")
    update_agent_status(AGENT_NAME, status="running")
    log.info("Intake agent online")

    while running:
        try:
            update_agent_status(AGENT_NAME, status="running")

            # Consume messages routed to us by the centralized poller
            events = consume_events("telegram_intake")

            for event in events:
                payload = json.loads(event["payload"]) if isinstance(event["payload"], str) else event["payload"]
                text = payload.get("text", "").strip()
                msg_id = payload.get("message_id")

                if not text:
                    continue

                # Skip if we already created a work order for this message
                if already_processed(msg_id):
                    log.debug(f"Skipping duplicate message_id={msg_id}")
                    continue

                job_data = parse_job_command(text)
                source = "command"

                if not job_data:
                    job_data = parse_freeform(text)
                    source = "freeform"

                if job_data:
                    wo_id = create_work_order(job_data, msg_id)
                    publish_event(AGENT_NAME, "work_order_created", {
                        "work_order_id": wo_id,
                        "customer": job_data["customer_name"],
                        "address": job_data.get("address", ""),
                        "description": job_data["description"],
                        "source": source,
                    })
                    reply = (
                        f"<b>New Work Order #{wo_id}</b>\n"
                        f"Customer: {job_data['customer_name']}\n"
                        f"Address: {job_data.get('address', 'N/A')}\n"
                        f"Issue: {job_data['description']}\n"
                        f"Status: <b>NEW</b> | Assigned to: JR"
                    )
                    send_telegram(reply)
                    log.info(f"Work order #{wo_id} created from {source}: {job_data['customer_name']}")

        except Exception as e:
            log.error(f"Intake agent error: {e}")
            update_agent_status(AGENT_NAME, status="error", error=str(e))

        time.sleep(INTERVAL)

    log.info("Intake agent stopped")


if __name__ == "__main__":
    main()
