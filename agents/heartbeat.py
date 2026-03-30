"""
Heartbeat Monitor Agent
Runs every 2 minutes. Checks agent_status table for each agent's last heartbeat.
If any agent hasn't checked in within 3x its expected interval, sends a Telegram alert.
"""

import sys
import os
import time
import signal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import update_agent_status, get_all_agent_statuses, publish_event, init_db
from shared.notify import send_alert, send_error
from shared.logger import get_logger

import yaml

AGENT_NAME = "heartbeat"
INTERVAL = 120  # 2 minutes

# Expected intervals per agent (seconds) — used to calculate 3x dead threshold
AGENT_INTERVALS = {
    "booking_watcher": 60,
    "prospect_nurture": 300,
    "intake_agent": 10,
    "status_updater": 10,
    "reminder_agent": 300,
    "daily_digest": 60,
    "log_janitor": 3600,
}

WATCHED_AGENTS = list(AGENT_INTERVALS.keys())

log = get_logger(AGENT_NAME)
running = True


def shutdown(signum, frame):
    global running
    log.info("Shutting down heartbeat monitor")
    update_agent_status(AGENT_NAME, status="stopped")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def check_agent_health():
    """Check all agents. Alert if any hasn't checked in within 3x its expected interval."""
    statuses = {a["agent_name"]: a for a in get_all_agent_statuses()}
    now = time.time()
    dead_agents = []

    for name in WATCHED_AGENTS:
        expected_interval = AGENT_INTERVALS[name]
        dead_threshold = expected_interval * 3  # 3x interval = considered dead

        agent = statuses.get(name)
        if not agent:
            # Agent has never reported — only flag if system has been up a while
            continue

        last_beat = agent.get("last_heartbeat")
        if last_beat and (now - last_beat) > dead_threshold:
            gap = int(now - last_beat)
            dead_agents.append(f"{name} (silent {gap}s, threshold {dead_threshold}s)")
            log.warning(f"Agent {name} missed heartbeat — {gap}s since last check-in (threshold: {dead_threshold}s)")

            publish_event(AGENT_NAME, "agent_down", {
                "agent": name,
                "last_heartbeat": last_beat,
                "gap_seconds": gap,
                "threshold": dead_threshold,
            })

    if dead_agents:
        msg = "⚠️ Agents not responding:\n" + "\n".join(f"  - {a}" for a in dead_agents)
        send_alert(AGENT_NAME, msg)
        log.warning(msg)

    alive_count = sum(1 for a in statuses.values()
                      if a["agent_name"] != AGENT_NAME and a.get("status") == "running")
    log.info(f"Health check: {alive_count}/{len(WATCHED_AGENTS)} agents alive, {len(dead_agents)} unresponsive")
    return len(dead_agents)


def main():
    init_db()
    log.info("Heartbeat monitor starting")
    update_agent_status(AGENT_NAME, status="running")
    send_alert(AGENT_NAME, "🕸️ BookedUp Web — Heartbeat Monitor online. Watching 8 agents.")

    cycle = 0
    while running:
        try:
            update_agent_status(AGENT_NAME, status="running")
            cycle += 1

            # Check health every cycle (every 2 minutes)
            check_agent_health()

            log.debug(f"Heartbeat cycle {cycle}")

        except Exception as e:
            log.error(f"Heartbeat error: {e}")
            update_agent_status(AGENT_NAME, status="error", error=str(e))
            send_error(AGENT_NAME, str(e))

        time.sleep(INTERVAL)

    log.info("Heartbeat monitor stopped")


if __name__ == "__main__":
    main()
