"""
Centralized Telegram Poller
Single process polls getUpdates and dispatches messages to agents via the events table.
Tracks update_id on disk so restarts never reprocess old messages.
"""

import sys
import os
import time
import signal
import requests
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import publish_event, get_connection, init_db
from shared.logger import get_logger

AGENT_NAME = "telegram_poller"
INTERVAL = 5
MEMORY_DIR = "/Users/bookedup/bookedup/memory"
OFFSET_FILE = os.path.join(MEMORY_DIR, "telegram_last_update_id.txt")

log = get_logger(AGENT_NAME)
running = True


def shutdown(signum, frame):
    global running
    log.info("Shutting down telegram poller")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def load_config():
    import yaml
    with open("/Users/bookedup/bookedup/config.yaml", "r") as f:
        return yaml.safe_load(f)


def get_last_update_id():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, "r") as f:
            return int(f.read().strip())
    return 0


def save_last_update_id(uid):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(uid))


def poll_telegram(token, offset):
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    params = {"offset": offset, "timeout": 5, "allowed_updates": ["message"]}
    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 409:
            # Another getUpdates session is active — back off, don't retry fast
            log.warning("409 Conflict — another poller is active, backing off 30s")
            time.sleep(30)
            return None  # Signal conflict, distinct from empty results
        resp.raise_for_status()
        data = resp.json()
        if data.get("ok"):
            return data.get("result", [])
    except requests.RequestException as e:
        log.error(f"Telegram poll error: {e}")
    return []


def is_already_dispatched(update_id):
    """Check if this Telegram update_id has already been dispatched to the events table."""
    conn = get_connection()
    row = conn.execute(
        "SELECT id FROM events WHERE source = ? AND payload LIKE ?",
        (AGENT_NAME, f'%"update_id": {update_id}%')
    ).fetchone()
    conn.close()
    return row is not None


def dispatch_message(text, update):
    """Route incoming message to the right agent via the events table."""
    update_id = update["update_id"]

    # Hard dedup: check if we already dispatched this update_id
    if is_already_dispatched(update_id):
        log.debug(f"Skipping already-dispatched update_id={update_id}")
        return

    msg = update.get("message", {})
    payload = {
        "update_id": update_id,
        "message_id": msg.get("message_id"),
        "chat_id": msg.get("chat", {}).get("id"),
        "from": msg.get("from", {}),
        "text": text,
    }

    tl = text.lower().strip()

    if tl.startswith("/job ") and "|" in text:
        publish_event(AGENT_NAME, "telegram_intake", payload)
        log.info(f"Dispatched to intake: {text[:50]}")
    elif tl.startswith("/status "):
        publish_event(AGENT_NAME, "telegram_status", payload)
        log.info(f"Dispatched to status_updater: {text[:50]}")
    elif tl.startswith("/note "):
        publish_event(AGENT_NAME, "telegram_status", payload)
    elif tl.startswith("/assign "):
        publish_event(AGENT_NAME, "telegram_status", payload)
    elif tl.startswith("/priority "):
        publish_event(AGENT_NAME, "telegram_status", payload)
    elif tl == "/jobs":
        publish_event(AGENT_NAME, "telegram_status", payload)
    elif tl.startswith("/job ") and "|" not in text:
        publish_event(AGENT_NAME, "telegram_status", payload)
    elif tl == "/digest":
        publish_event(AGENT_NAME, "telegram_digest", payload)
    elif tl == "/bookings":
        publish_event(AGENT_NAME, "telegram_bookings", payload)
    elif tl.startswith("/prospect"):
        publish_event(AGENT_NAME, "telegram_prospect", payload)
    elif tl == "/prospects":
        publish_event(AGENT_NAME, "telegram_prospect", payload)
    else:
        # Freeform — dispatch to intake for keyword parsing
        publish_event(AGENT_NAME, "telegram_intake", payload)
        log.debug(f"Dispatched freeform to intake: {text[:50]}")


def main():
    init_db()
    log.info("Centralized Telegram poller starting")

    cfg = load_config()
    token = cfg["telegram"]["bot_token"]
    offset = get_last_update_id() + 1

    while running:
        try:
            updates = poll_telegram(token, offset)

            # None = 409 conflict, already backed off
            if updates is None:
                continue

            for update in updates:
                update_id = update["update_id"]

                # Always advance offset FIRST, before dispatching
                offset = update_id + 1
                save_last_update_id(update_id)

                msg = update.get("message", {})
                text = msg.get("text", "").strip()
                if not text:
                    continue

                dispatch_message(text, update)

        except Exception as e:
            log.error(f"Telegram poller error: {e}")

        time.sleep(INTERVAL)

    log.info("Telegram poller stopped")


if __name__ == "__main__":
    main()
