"""
Booking Watcher Agent
Monitors Cal.com for BookedUp's own bookings. Reads /bookings command
from centralized Telegram poller via events table.
"""

import sys
import os
import time
import signal
import json
import requests
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import update_agent_status, publish_event, consume_events, get_connection, init_db
from shared.notify import send_alert, send_telegram
from shared.logger import get_logger

AGENT_NAME = "booking_watcher"
INTERVAL = 60
MEMORY_DIR = "/Users/bookedup/bookedup/memory/booking_watcher"

log = get_logger(AGENT_NAME)
running = True


def shutdown(signum, frame):
    global running
    log.info("Shutting down booking watcher")
    update_agent_status(AGENT_NAME, status="stopped")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def init_bookings_table():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT UNIQUE,
            created_at REAL NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            title TEXT,
            attendee_name TEXT,
            attendee_email TEXT,
            event_type TEXT,
            status TEXT DEFAULT 'confirmed',
            source TEXT DEFAULT 'cal.com',
            raw_data TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_bookings_external ON bookings(external_id);
        CREATE INDEX IF NOT EXISTS idx_bookings_start ON bookings(start_time);
    """)
    conn.commit()
    conn.close()


def load_config():
    import yaml
    with open("/Users/bookedup/bookedup/config.yaml", "r") as f:
        return yaml.safe_load(f)


def get_last_check_time():
    path = os.path.join(MEMORY_DIR, "last_check.txt")
    if os.path.exists(path):
        with open(path, "r") as f:
            return float(f.read().strip())
    return time.time() - 86400


def save_last_check_time(ts):
    path = os.path.join(MEMORY_DIR, "last_check.txt")
    with open(path, "w") as f:
        f.write(str(ts))


def fetch_calcom_bookings(api_key):
    if not api_key:
        return []
    url = "https://api.cal.com/v1/bookings"
    params = {"apiKey": api_key}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("bookings", [])
    except requests.RequestException as e:
        log.error(f"Cal.com API error: {e}")
        return []


def process_booking(booking):
    ext_id = str(booking.get("id", booking.get("uid", "")))
    conn = get_connection()
    if conn.execute("SELECT id FROM bookings WHERE external_id = ?", (ext_id,)).fetchone():
        conn.close()
        return None
    attendees = booking.get("attendees", [{}])
    attendee = attendees[0] if attendees else {}
    start = booking.get("startTime", "")
    end = booking.get("endTime", "")
    title = booking.get("title", "Booking")
    conn.execute(
        """INSERT INTO bookings
           (external_id, created_at, start_time, end_time, title,
            attendee_name, attendee_email, event_type, status, raw_data)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (ext_id, time.time(), start, end, title, attendee.get("name", "Unknown"),
         attendee.get("email", ""), booking.get("eventType", {}).get("slug", "unknown"),
         booking.get("status", "confirmed"), json.dumps(booking))
    )
    conn.commit()
    conn.close()
    return {
        "external_id": ext_id, "title": title,
        "attendee_name": attendee.get("name", "Unknown"),
        "attendee_email": attendee.get("email", ""),
        "start_time": start, "end_time": end,
    }


def handle_bookings_list():
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM bookings WHERE status = 'confirmed' ORDER BY start_time ASC LIMIT 10"
    ).fetchall()
    conn.close()
    if not rows:
        return "No upcoming bookings."
    lines = ["<b>Upcoming Bookings:</b>"]
    for r in rows:
        start = r["start_time"][:16] if r["start_time"] else "TBD"
        lines.append(f"  📅 {r['title']} — {r['attendee_name']}\n      {start}")
    return "\n".join(lines)


def main():
    init_db()
    init_bookings_table()
    log.info("Booking watcher starting")
    update_agent_status(AGENT_NAME, status="running")

    cfg = load_config()
    calcom_key = cfg.get("calcom", {}).get("api_key", "")

    if not calcom_key:
        log.warning("No Cal.com API key configured — running in Telegram-only mode")
    else:
        log.info("Booking watcher online — monitoring Cal.com")

    while running:
        try:
            update_agent_status(AGENT_NAME, status="running")

            # Poll Cal.com if configured
            if calcom_key:
                bookings = fetch_calcom_bookings(calcom_key)
                for b in bookings:
                    result = process_booking(b)
                    if result:
                        msg = (
                            f"📅 <b>New Booking!</b>\n{result['title']}\n"
                            f"With: {result['attendee_name']} ({result['attendee_email']})\n"
                            f"When: {result['start_time'][:16]}"
                        )
                        send_telegram(msg)
                        publish_event(AGENT_NAME, "new_booking", result)
                        log.info(f"New booking: {result['attendee_name']} - {result['title']}")
                save_last_check_time(time.time())

            # Check for /bookings command via events
            events = consume_events("telegram_bookings")
            for event in events:
                reply = handle_bookings_list()
                send_telegram(reply)

        except Exception as e:
            log.error(f"Booking watcher error: {e}")
            update_agent_status(AGENT_NAME, status="error", error=str(e))

        time.sleep(INTERVAL)

    log.info("Booking watcher stopped")


if __name__ == "__main__":
    main()
