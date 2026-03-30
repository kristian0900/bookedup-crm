"""
Prospect Nurture Agent
Manages drip sequences for BookedUp's 25 East Valley prospects.
Reads commands from centralized Telegram poller via events table.
"""

import sys
import os
import time
import signal
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import update_agent_status, publish_event, consume_events, get_connection, init_db
from shared.notify import send_telegram, send_alert
from shared.logger import get_logger

AGENT_NAME = "prospect_nurture"
INTERVAL = 60
MEMORY_DIR = "/Users/bookedup/bookedup/memory/prospect_nurture"

log = get_logger(AGENT_NAME)
running = True

DRIP_SEQUENCE = [
    {"step": 1, "day": 0,  "action": "Initial outreach — intro message + value prop"},
    {"step": 2, "day": 3,  "action": "Follow-up — share a quick win or case study"},
    {"step": 3, "day": 7,  "action": "Value add — send relevant tip or industry insight"},
    {"step": 4, "day": 14, "action": "Check-in — ask if they have questions, offer demo"},
    {"step": 5, "day": 21, "action": "Social proof — share testimonial or results"},
    {"step": 6, "day": 30, "action": "Final push — limited-time offer or personal invite"},
    {"step": 7, "day": 45, "action": "Long-term nurture — monthly check-in begins"},
]


def shutdown(signum, frame):
    global running
    log.info("Shutting down prospect nurture")
    update_agent_status(AGENT_NAME, status="stopped")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def init_prospects_table():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS prospects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            business_name TEXT,
            phone TEXT,
            email TEXT,
            industry TEXT,
            location TEXT DEFAULT 'East Valley',
            status TEXT DEFAULT 'active',
            current_step INTEGER DEFAULT 0,
            added_at REAL NOT NULL,
            last_contact_at REAL,
            next_touch_at REAL,
            notes TEXT DEFAULT '',
            tags TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_prospects_status ON prospects(status);
        CREATE INDEX IF NOT EXISTS idx_prospects_next ON prospects(next_touch_at);
    """)
    conn.commit()
    conn.close()


def calculate_next_touch(added_at, current_step):
    if current_step >= len(DRIP_SEQUENCE):
        return time.time() + (30 * 86400)
    return added_at + (DRIP_SEQUENCE[current_step]["day"] * 86400)


def check_due_touchpoints():
    now = time.time()
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM prospects WHERE status = 'active' AND next_touch_at <= ?", (now,)
    ).fetchall()
    conn.close()
    due = []
    for r in rows:
        step_idx = r["current_step"]
        if step_idx < len(DRIP_SEQUENCE):
            action = DRIP_SEQUENCE[step_idx]["action"]
            step_num = DRIP_SEQUENCE[step_idx]["step"]
        else:
            action = "Monthly check-in (long-term nurture)"
            step_num = step_idx + 1
        due.append({
            "prospect_id": r["id"], "name": r["name"],
            "business": r["business_name"] or "",
            "step": step_num, "action": action,
            "days_since_added": int((now - r["added_at"]) / 86400),
        })
    return due


def advance_prospect(prospect_id):
    conn = get_connection()
    row = conn.execute("SELECT * FROM prospects WHERE id = ?", (prospect_id,)).fetchone()
    if not row:
        conn.close()
        return
    new_step = row["current_step"] + 1
    next_touch = calculate_next_touch(row["added_at"], new_step)
    conn.execute(
        "UPDATE prospects SET current_step = ?, last_contact_at = ?, next_touch_at = ? WHERE id = ?",
        (new_step, time.time(), next_touch, prospect_id)
    )
    conn.commit()
    conn.close()


def handle_add_prospect(args):
    parts = [p.strip() for p in args.split("|")]
    if not parts or not parts[0]:
        return ("Usage: /prospect add Name | Business | Phone | Email | Industry\n"
                "Minimum: /prospect add Name")
    name = parts[0]
    business = parts[1] if len(parts) > 1 else ""
    phone = parts[2] if len(parts) > 2 else ""
    email = parts[3] if len(parts) > 3 else ""
    industry = parts[4] if len(parts) > 4 else ""
    now = time.time()
    next_touch = calculate_next_touch(now, 0)
    conn = get_connection()
    cursor = conn.execute(
        "INSERT INTO prospects (name, business_name, phone, email, industry, added_at, next_touch_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (name, business, phone, email, industry, now, next_touch)
    )
    pid = cursor.lastrowid
    conn.commit()
    conn.close()
    publish_event(AGENT_NAME, "prospect_added", {"prospect_id": pid, "name": name, "business": business})
    log.info(f"Added prospect #{pid}: {name}")
    return (f"<b>Prospect #{pid} added</b>\nName: {name}\n"
            f"Business: {business or 'N/A'}\nDrip sequence started — Step 1 due now")


def handle_list_prospects():
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM prospects WHERE status = 'active' ORDER BY next_touch_at ASC"
    ).fetchall()
    conn.close()
    if not rows:
        return "No active prospects. Use /prospect add <name> to add one."
    lines = [f"<b>Active Prospects ({len(rows)}):</b>"]
    now = time.time()
    for r in rows:
        step = r["current_step"] + 1
        overdue = ""
        if r["next_touch_at"] and r["next_touch_at"] < now:
            days_over = int((now - r["next_touch_at"]) / 86400)
            overdue = f" ⚠️ {days_over}d overdue" if days_over > 0 else " 🔔 DUE"
        lines.append(
            f"  #{r['id']} {r['name']}"
            f"{' — ' + r['business_name'] if r['business_name'] else ''}"
            f" | Step {step}/{len(DRIP_SEQUENCE)}{overdue}"
        )
    return "\n".join(lines)


def handle_done_prospect(args):
    try:
        pid = int(args.strip())
    except ValueError:
        return "Usage: /prospect done <id>"
    conn = get_connection()
    row = conn.execute("SELECT * FROM prospects WHERE id = ?", (pid,)).fetchone()
    conn.close()
    if not row:
        return f"Prospect #{pid} not found."
    advance_prospect(pid)
    step = row["current_step"] + 1
    next_step = step + 1
    if next_step <= len(DRIP_SEQUENCE):
        next_action = DRIP_SEQUENCE[step]["action"]
        next_day = DRIP_SEQUENCE[step]["day"]
        return (f"✓ Prospect #{pid} ({row['name']}) — Step {step} complete!\n"
                f"Next: Step {next_step} on day {next_day} — {next_action}")
    else:
        return (f"✓ Prospect #{pid} ({row['name']}) — Step {step} complete!\n"
                f"Drip sequence finished. Moving to monthly nurture.")


def handle_prospect_pause(args):
    try:
        pid = int(args.strip())
    except ValueError:
        return "Usage: /prospect pause <id>"
    conn = get_connection()
    conn.execute("UPDATE prospects SET status = 'paused' WHERE id = ?", (pid,))
    conn.commit()
    conn.close()
    return f"Prospect #{pid} paused. Use /prospect resume {pid} to reactivate."


def handle_prospect_resume(args):
    try:
        pid = int(args.strip())
    except ValueError:
        return "Usage: /prospect resume <id>"
    conn = get_connection()
    conn.execute("UPDATE prospects SET status = 'active', next_touch_at = ? WHERE id = ?", (time.time(), pid))
    conn.commit()
    conn.close()
    return f"Prospect #{pid} reactivated. Next touchpoint due now."


def main():
    init_db()
    init_prospects_table()
    log.info("Prospect nurture agent starting")
    update_agent_status(AGENT_NAME, status="running")
    log.info("Prospect nurture online")

    while running:
        try:
            update_agent_status(AGENT_NAME, status="running")

            # Check for due touchpoints
            due = check_due_touchpoints()
            for d in due:
                msg = (
                    f"🎯 <b>Prospect Touchpoint Due</b>\n"
                    f"#{d['prospect_id']} {d['name']}"
                    f"{' — ' + d['business'] if d['business'] else ''}\n"
                    f"Step {d['step']}: {d['action']}\n"
                    f"Day {d['days_since_added']} in sequence\n"
                    f"→ Use /prospect done {d['prospect_id']} when complete"
                )
                send_telegram(msg)
                publish_event(AGENT_NAME, "touchpoint_due", d)
                log.info(f"Touchpoint due: prospect #{d['prospect_id']} step {d['step']}")

            # Check for commands via events
            events = consume_events("telegram_prospect")
            for event in events:
                payload = json.loads(event["payload"]) if isinstance(event["payload"], str) else event["payload"]
                text = payload.get("text", "").strip()
                if not text:
                    continue
                reply = None
                tl = text.lower()
                if tl.startswith("/prospect add "):
                    reply = handle_add_prospect(text[14:])
                elif tl == "/prospects":
                    reply = handle_list_prospects()
                elif tl.startswith("/prospect done "):
                    reply = handle_done_prospect(text[15:])
                elif tl.startswith("/prospect pause "):
                    reply = handle_prospect_pause(text[16:])
                elif tl.startswith("/prospect resume "):
                    reply = handle_prospect_resume(text[17:])
                if reply:
                    send_telegram(reply)

        except Exception as e:
            log.error(f"Prospect nurture error: {e}")
            update_agent_status(AGENT_NAME, status="error", error=str(e))

        time.sleep(INTERVAL)

    log.info("Prospect nurture stopped")


if __name__ == "__main__":
    main()
