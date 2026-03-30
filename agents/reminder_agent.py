"""
Reminder Agent
Follows up on work orders that need attention:
- Jobs stuck in 'new' for too long (no one scheduled them)
- Jobs 'in_progress' for too long (might be stalled)
- Completed jobs that need follow-up (customer satisfaction)
- Cancelled/abandoned jobs worth revisiting
"""

import sys
import os
import time
import signal
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import update_agent_status, publish_event, get_connection, init_db
from shared.notify import send_telegram, send_alert
from shared.logger import get_logger

AGENT_NAME = "reminder_agent"
INTERVAL = 300  # every 5 minutes
MEMORY_DIR = "/Users/bookedup/bookedup/memory/reminder_agent"

# Thresholds in seconds
NEW_JOB_REMINDER = 3600         # 1 hour — unscheduled new job
IN_PROGRESS_REMINDER = 86400    # 24 hours — job might be stalled
COMPLETED_FOLLOWUP = 172800     # 48 hours — follow up after completion
REMINDER_COOLDOWN = 3600        # Don't re-remind within 1 hour

log = get_logger(AGENT_NAME)
running = True


def shutdown(signum, frame):
    global running
    log.info("Shutting down reminder agent")
    update_agent_status(AGENT_NAME, status="stopped")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def load_reminded():
    """Load set of recently reminded work order IDs with timestamps."""
    import json
    path = os.path.join(MEMORY_DIR, "reminded.json")
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}


def save_reminded(reminded):
    import json
    path = os.path.join(MEMORY_DIR, "reminded.json")
    with open(path, "w") as f:
        json.dump(reminded, f)


def should_remind(wo_id, reminded):
    """Check if enough time has passed since last reminder for this WO."""
    key = str(wo_id)
    if key not in reminded:
        return True
    return (time.time() - reminded[key]) > REMINDER_COOLDOWN


def check_new_jobs(conn, reminded):
    """Find jobs stuck in 'new' status too long."""
    cutoff = time.time() - NEW_JOB_REMINDER
    rows = conn.execute(
        "SELECT * FROM work_orders WHERE status = 'new' AND created_at < ?",
        (cutoff,)
    ).fetchall()

    reminders = []
    for r in rows:
        if should_remind(r["id"], reminded):
            age_hrs = (time.time() - r["created_at"]) / 3600
            reminders.append({
                "wo_id": r["id"],
                "type": "unscheduled",
                "message": (
                    f"⏰ <b>WO #{r['id']}</b> has been sitting in NEW for "
                    f"{age_hrs:.1f} hours\n"
                    f"Customer: {r['customer_name']}\n"
                    f"Issue: {r['description']}\n"
                    f"→ Use /status {r['id']} scheduled to acknowledge"
                ),
            })
    return reminders


def check_stalled_jobs(conn, reminded):
    """Find jobs in_progress too long."""
    cutoff = time.time() - IN_PROGRESS_REMINDER
    rows = conn.execute(
        "SELECT * FROM work_orders WHERE status = 'in_progress' AND updated_at < ?",
        (cutoff,)
    ).fetchall()

    reminders = []
    for r in rows:
        if should_remind(r["id"], reminded):
            days = (time.time() - r["updated_at"]) / 86400
            reminders.append({
                "wo_id": r["id"],
                "type": "stalled",
                "message": (
                    f"⚠️ <b>WO #{r['id']}</b> has been in progress for "
                    f"{days:.1f} days with no update\n"
                    f"Customer: {r['customer_name']}\n"
                    f"→ Use /note {r['id']} <update> or /status {r['id']} completed"
                ),
            })
    return reminders


def check_followups(conn, reminded):
    """Find completed jobs ready for follow-up."""
    cutoff = time.time() - COMPLETED_FOLLOWUP
    # Only follow up on recently completed jobs (within last week)
    recent = time.time() - (7 * 86400)
    rows = conn.execute(
        "SELECT * FROM work_orders WHERE status = 'completed' "
        "AND completed_at IS NOT NULL AND completed_at < ? AND completed_at > ?",
        (cutoff, recent)
    ).fetchall()

    reminders = []
    for r in rows:
        if should_remind(r["id"], reminded):
            reminders.append({
                "wo_id": r["id"],
                "type": "followup",
                "message": (
                    f"📋 <b>Follow-up:</b> WO #{r['id']} completed — "
                    f"time to check in with {r['customer_name']}?\n"
                    f"Job: {r['description']}"
                ),
            })
    return reminders


def main():
    init_db()
    log.info("Reminder agent starting")
    update_agent_status(AGENT_NAME, status="running")
    log.info("Reminder agent online")

    while running:
        try:
            update_agent_status(AGENT_NAME, status="running")
            reminded = load_reminded()
            conn = get_connection()

            all_reminders = []
            all_reminders.extend(check_new_jobs(conn, reminded))
            all_reminders.extend(check_stalled_jobs(conn, reminded))
            all_reminders.extend(check_followups(conn, reminded))
            conn.close()

            for reminder in all_reminders:
                send_telegram(reminder["message"])
                reminded[str(reminder["wo_id"])] = time.time()

                publish_event(AGENT_NAME, "reminder_sent", {
                    "work_order_id": reminder["wo_id"],
                    "reminder_type": reminder["type"],
                })

                log.info(f"Reminder sent: WO #{reminder['wo_id']} ({reminder['type']})")

            # Clean up old reminded entries (older than 7 days)
            cutoff = time.time() - (7 * 86400)
            reminded = {k: v for k, v in reminded.items() if v > cutoff}
            save_reminded(reminded)

            if all_reminders:
                log.info(f"Sent {len(all_reminders)} reminders this cycle")

        except Exception as e:
            log.error(f"Reminder agent error: {e}")
            update_agent_status(AGENT_NAME, status="error", error=str(e))

        time.sleep(INTERVAL)

    log.info("Reminder agent stopped")


if __name__ == "__main__":
    main()
