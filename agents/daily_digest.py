"""
Daily Digest Agent — End-of-day summary for Ace
Runs on schedule (default 8:00 AM) and sends a Telegram summary.
Also responds to /digest command via centralized poller events.
"""

import sys
import os
import time
import signal
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import (
    update_agent_status, publish_event, consume_events, get_connection,
    peek_events, init_db
)
from shared.notify import send_daily_digest
from shared.logger import get_logger

AGENT_NAME = "daily_digest"
INTERVAL = 30
MEMORY_DIR = "/Users/bookedup/bookedup/memory/daily_digest"

log = get_logger(AGENT_NAME)
running = True


def shutdown(signum, frame):
    global running
    log.info("Shutting down daily digest")
    update_agent_status(AGENT_NAME, status="stopped")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def get_last_digest_date():
    path = os.path.join(MEMORY_DIR, "last_digest.txt")
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    return ""


def save_last_digest_date(date_str):
    path = os.path.join(MEMORY_DIR, "last_digest.txt")
    with open(path, "w") as f:
        f.write(date_str)


def load_config():
    import yaml
    with open("/Users/bookedup/bookedup/config.yaml", "r") as f:
        return yaml.safe_load(f)


def build_digest():
    conn = get_connection()
    now = time.time()
    day_ago = now - 86400

    new_orders = conn.execute(
        "SELECT * FROM work_orders WHERE created_at > ? ORDER BY created_at DESC", (day_ago,)
    ).fetchall()
    completed = conn.execute(
        "SELECT * FROM work_orders WHERE completed_at IS NOT NULL AND completed_at > ?", (day_ago,)
    ).fetchall()
    open_orders = conn.execute(
        "SELECT * FROM work_orders WHERE status NOT IN ('completed', 'cancelled') "
        "ORDER BY priority DESC, created_at ASC"
    ).fetchall()
    errors = peek_events(event_type="agent_down", since=day_ago)
    reminders = peek_events(event_type="reminder_sent", since=day_ago)
    conn.close()

    today = datetime.now().strftime("%A, %B %d")
    lines = [f"📊 <b>{today}</b>\n"]
    lines.append(f"<b>New Jobs (24h):</b> {len(new_orders)}")
    for wo in new_orders[:10]:
        lines.append(f"  #{wo['id']} {wo['customer_name']} — {wo['description'][:50]}")
    lines.append("")
    lines.append(f"<b>Completed (24h):</b> {len(completed)}")
    for wo in completed[:10]:
        lines.append(f"  #{wo['id']} {wo['customer_name']} ✓")
    lines.append("")
    lines.append(f"<b>Open Jobs:</b> {len(open_orders)}")
    status_counts = {}
    for wo in open_orders:
        s = wo["status"]
        status_counts[s] = status_counts.get(s, 0) + 1
    for s, count in sorted(status_counts.items()):
        lines.append(f"  {s}: {count}")
    if open_orders:
        high_priority = [wo for wo in open_orders if wo["priority"] == "high"]
        if high_priority:
            lines.append("\n<b>⚡ High Priority:</b>")
            for wo in high_priority:
                lines.append(f"  #{wo['id']} {wo['customer_name']} — {wo['status']}")
    if errors:
        lines.append(f"\n⚠️ <b>Agent alerts:</b> {len(errors)} in last 24h")
    if reminders:
        lines.append(f"📬 <b>Reminders sent:</b> {len(reminders)} in last 24h")
    return "\n".join(lines)


def main():
    init_db()
    log.info("Daily digest agent starting")
    update_agent_status(AGENT_NAME, status="running")

    cfg = load_config()
    schedule_time = cfg.get("agents", {}).get("daily_digest", {}).get("schedule", "08:00")
    schedule_hour, schedule_min = map(int, schedule_time.split(":"))

    while running:
        try:
            update_agent_status(AGENT_NAME, status="running")

            # Check for /digest command via events
            events = consume_events("telegram_digest")
            for event in events:
                log.info("On-demand digest requested")
                digest = build_digest()
                send_daily_digest(digest)

            # Scheduled digest
            now = datetime.now()
            today_str = now.strftime("%Y-%m-%d")
            if (now.hour == schedule_hour and now.minute >= schedule_min and
                    now.minute < schedule_min + 5 and get_last_digest_date() != today_str):
                log.info("Sending scheduled daily digest")
                digest = build_digest()
                send_daily_digest(digest)
                save_last_digest_date(today_str)
                publish_event(AGENT_NAME, "daily_digest_sent", {"date": today_str, "type": "scheduled"})

        except Exception as e:
            log.error(f"Daily digest error: {e}")
            update_agent_status(AGENT_NAME, status="error", error=str(e))

        time.sleep(INTERVAL)

    log.info("Daily digest stopped")


if __name__ == "__main__":
    main()
