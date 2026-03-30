"""
Status Updater Agent
Reads commands from the centralized Telegram poller (via events table).
Lets JR update work order status via Telegram text commands.

Commands:
  /status <id> <new_status>     — Update job status
  /note <id> <note text>        — Add a note to a job
  /assign <id> <person>         — Reassign a job
  /priority <id> high|low|normal — Change priority
  /jobs                          — List open jobs
  /job <id>                      — View job details
"""

import sys
import os
import time
import signal
import json
import re
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import update_agent_status, publish_event, consume_events, get_connection, init_db
from shared.notify import send_telegram, send_alert
from shared.logger import get_logger

AGENT_NAME = "status_updater"
INTERVAL = 5

VALID_STATUSES = ["new", "scheduled", "in_progress", "on_hold", "completed", "cancelled"]

log = get_logger(AGENT_NAME)
running = True


def shutdown(signum, frame):
    global running
    log.info("Shutting down status updater")
    update_agent_status(AGENT_NAME, status="stopped")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def handle_status(args):
    match = re.match(r"(\d+)\s+(\w+)", args)
    if not match:
        return "Usage: /status <job_id> <new_status>\nStatuses: " + ", ".join(VALID_STATUSES)
    wo_id, new_status = int(match.group(1)), match.group(2).lower()
    if new_status not in VALID_STATUSES:
        return f"Invalid status. Choose from: {', '.join(VALID_STATUSES)}"
    conn = get_connection()
    row = conn.execute("SELECT * FROM work_orders WHERE id = ?", (wo_id,)).fetchone()
    if not row:
        conn.close()
        return f"Work order #{wo_id} not found."
    old_status = row["status"]
    updates = {"status": new_status, "updated_at": time.time()}
    if new_status == "completed":
        updates["completed_at"] = time.time()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    conn.execute(f"UPDATE work_orders SET {set_clause} WHERE id = ?", list(updates.values()) + [wo_id])
    conn.commit()
    conn.close()
    publish_event(AGENT_NAME, "work_order_updated", {
        "work_order_id": wo_id, "field": "status", "old_value": old_status, "new_value": new_status,
    })
    log.info(f"WO #{wo_id}: {old_status} -> {new_status}")
    return f"Work Order #{wo_id} updated: <b>{old_status}</b> → <b>{new_status}</b>"


def handle_note(args):
    match = re.match(r"(\d+)\s+(.+)", args, re.DOTALL)
    if not match:
        return "Usage: /note <job_id> <note text>"
    wo_id, note = int(match.group(1)), match.group(2).strip()
    ts = datetime.now().strftime("%m/%d %H:%M")
    conn = get_connection()
    row = conn.execute("SELECT notes FROM work_orders WHERE id = ?", (wo_id,)).fetchone()
    if not row:
        conn.close()
        return f"Work order #{wo_id} not found."
    new_notes = f"{row['notes'] or ''}\n[{ts}] {note}".strip()
    conn.execute("UPDATE work_orders SET notes = ?, updated_at = ? WHERE id = ?", (new_notes, time.time(), wo_id))
    conn.commit()
    conn.close()
    publish_event(AGENT_NAME, "work_order_updated", {"work_order_id": wo_id, "field": "notes", "note": note})
    log.info(f"Note added to WO #{wo_id}")
    return f"Note added to Work Order #{wo_id}"


def handle_assign(args):
    match = re.match(r"(\d+)\s+(.+)", args)
    if not match:
        return "Usage: /assign <job_id> <person>"
    wo_id, person = int(match.group(1)), match.group(2).strip()
    conn = get_connection()
    row = conn.execute("SELECT assigned_to FROM work_orders WHERE id = ?", (wo_id,)).fetchone()
    if not row:
        conn.close()
        return f"Work order #{wo_id} not found."
    old = row["assigned_to"]
    conn.execute("UPDATE work_orders SET assigned_to = ?, updated_at = ? WHERE id = ?", (person, time.time(), wo_id))
    conn.commit()
    conn.close()
    publish_event(AGENT_NAME, "work_order_updated", {
        "work_order_id": wo_id, "field": "assigned_to", "old_value": old, "new_value": person,
    })
    log.info(f"WO #{wo_id} reassigned: {old} -> {person}")
    return f"Work Order #{wo_id} reassigned: <b>{old}</b> → <b>{person}</b>"


def handle_priority(args):
    match = re.match(r"(\d+)\s+(\w+)", args)
    if not match:
        return "Usage: /priority <job_id> high|low|normal"
    wo_id, priority = int(match.group(1)), match.group(2).lower()
    if priority not in ("high", "low", "normal"):
        return "Priority must be: high, low, or normal"
    conn = get_connection()
    row = conn.execute("SELECT id FROM work_orders WHERE id = ?", (wo_id,)).fetchone()
    if not row:
        conn.close()
        return f"Work order #{wo_id} not found."
    conn.execute("UPDATE work_orders SET priority = ?, updated_at = ? WHERE id = ?", (priority, time.time(), wo_id))
    conn.commit()
    conn.close()
    log.info(f"WO #{wo_id} priority -> {priority}")
    return f"Work Order #{wo_id} priority set to <b>{priority}</b>"


def handle_jobs():
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, customer_name, status, priority, assigned_to FROM work_orders "
        "WHERE status NOT IN ('completed', 'cancelled') ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    conn.close()
    if not rows:
        return "No open work orders."
    lines = ["<b>Open Work Orders:</b>"]
    for r in rows:
        flag = "🔴" if r["priority"] == "high" else "🟡" if r["priority"] == "normal" else "⚪"
        lines.append(f"  #{r['id']} {flag} {r['customer_name']} — <i>{r['status']}</i> ({r['assigned_to']})")
    return "\n".join(lines)


def handle_job_detail(args):
    match = re.match(r"^(\d+)$", args.strip())
    if not match:
        return None
    wo_id = int(match.group(1))
    conn = get_connection()
    row = conn.execute("SELECT * FROM work_orders WHERE id = ?", (wo_id,)).fetchone()
    conn.close()
    if not row:
        return f"Work order #{wo_id} not found."
    created = datetime.fromtimestamp(row["created_at"]).strftime("%m/%d/%Y %I:%M %p")
    detail = (
        f"<b>Work Order #{row['id']}</b>\n"
        f"Customer: {row['customer_name']}\nAddress: {row['address'] or 'N/A'}\n"
        f"Issue: {row['description']}\nStatus: <b>{row['status']}</b>\n"
        f"Priority: {row['priority']}\nAssigned to: {row['assigned_to']}\nCreated: {created}\n"
    )
    if row["notes"]:
        detail += f"\nNotes:\n{row['notes']}"
    return detail


def main():
    init_db()
    log.info("Status updater starting")
    update_agent_status(AGENT_NAME, status="running")
    log.info("Status updater online")

    while running:
        try:
            update_agent_status(AGENT_NAME, status="running")

            events = consume_events("telegram_status")
            for event in events:
                payload = json.loads(event["payload"]) if isinstance(event["payload"], str) else event["payload"]
                text = payload.get("text", "").strip()
                if not text:
                    continue

                reply = None
                tl = text.lower()
                if tl.startswith("/status "):
                    reply = handle_status(text[8:])
                elif tl.startswith("/note "):
                    reply = handle_note(text[6:])
                elif tl.startswith("/assign "):
                    reply = handle_assign(text[8:])
                elif tl.startswith("/priority "):
                    reply = handle_priority(text[10:])
                elif tl == "/jobs":
                    reply = handle_jobs()
                elif tl.startswith("/job "):
                    reply = handle_job_detail(text[5:])

                if reply:
                    send_telegram(reply)

        except Exception as e:
            log.error(f"Status updater error: {e}")
            update_agent_status(AGENT_NAME, status="error", error=str(e))

        time.sleep(INTERVAL)

    log.info("Status updater stopped")


if __name__ == "__main__":
    main()
