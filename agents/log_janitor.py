"""
Log Janitor Agent
Rotates and cleans log files, prunes old events from the database,
and keeps the memory folders from growing unbounded.
"""

import sys
import os
import time
import signal
import glob
import shutil
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db import update_agent_status, publish_event, get_connection, init_db
from shared.notify import send_alert
from shared.logger import get_logger

AGENT_NAME = "log_janitor"
INTERVAL = 3600  # every hour
LOG_DIR = "/Users/bookedup/bookedup/logs"
MEMORY_DIR = "/Users/bookedup/bookedup/memory"
MAX_LOG_SIZE_MB = 10
MAX_EVENT_AGE_DAYS = 30
MAX_MEMORY_FILE_AGE_DAYS = 90

log = get_logger(AGENT_NAME)
running = True


def shutdown(signum, frame):
    global running
    log.info("Shutting down log janitor")
    update_agent_status(AGENT_NAME, status="stopped")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def rotate_logs():
    """Rotate any log file over MAX_LOG_SIZE_MB."""
    rotated = 0
    for log_file in glob.glob(os.path.join(LOG_DIR, "*.log")):
        size_mb = os.path.getsize(log_file) / (1024 * 1024)
        if size_mb > MAX_LOG_SIZE_MB:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive = f"{log_file}.{ts}"
            shutil.move(log_file, archive)
            # Touch a new empty log file
            open(log_file, "w").close()
            rotated += 1
            log.info(f"Rotated {os.path.basename(log_file)} ({size_mb:.1f}MB)")

    # Clean up old rotated logs (keep last 3 per agent)
    for base_log in glob.glob(os.path.join(LOG_DIR, "*.log")):
        pattern = f"{base_log}.*"
        archives = sorted(glob.glob(pattern), reverse=True)
        for old in archives[3:]:
            os.remove(old)
            log.info(f"Deleted old archive: {os.path.basename(old)}")

    return rotated


def prune_events():
    """Delete consumed events older than MAX_EVENT_AGE_DAYS."""
    cutoff = time.time() - (MAX_EVENT_AGE_DAYS * 86400)
    conn = get_connection()
    cursor = conn.execute(
        "DELETE FROM events WHERE consumed = 1 AND timestamp < ?", (cutoff,)
    )
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted > 0:
        log.info(f"Pruned {deleted} old events from database")
    return deleted


def clean_memory():
    """Remove stale files from agent memory folders."""
    cleaned = 0
    cutoff = time.time() - (MAX_MEMORY_FILE_AGE_DAYS * 86400)

    for agent_dir in os.listdir(MEMORY_DIR):
        agent_path = os.path.join(MEMORY_DIR, agent_dir)
        if not os.path.isdir(agent_path):
            continue
        for fname in os.listdir(agent_path):
            fpath = os.path.join(agent_path, fname)
            if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                os.remove(fpath)
                cleaned += 1
                log.info(f"Cleaned stale memory file: {agent_dir}/{fname}")

    return cleaned


def get_disk_stats():
    """Return summary of disk usage for logs and memory."""
    log_size = sum(
        os.path.getsize(f) for f in glob.glob(os.path.join(LOG_DIR, "*")) if os.path.isfile(f)
    )
    mem_size = 0
    for root, dirs, files in os.walk(MEMORY_DIR):
        mem_size += sum(os.path.getsize(os.path.join(root, f)) for f in files)

    db_path = "/Users/bookedup/bookedup/shared/bookedup.db"
    db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0

    return {
        "logs_mb": round(log_size / (1024 * 1024), 2),
        "memory_mb": round(mem_size / (1024 * 1024), 2),
        "db_mb": round(db_size / (1024 * 1024), 2),
    }


def main():
    init_db()
    log.info("Log janitor starting")
    update_agent_status(AGENT_NAME, status="running")

    while running:
        try:
            update_agent_status(AGENT_NAME, status="running")

            rotated = rotate_logs()
            pruned = prune_events()
            cleaned = clean_memory()
            stats = get_disk_stats()

            summary = {
                "rotated_logs": rotated,
                "pruned_events": pruned,
                "cleaned_memory_files": cleaned,
                "disk": stats,
            }

            log.info(
                f"Cleanup done: {rotated} logs rotated, {pruned} events pruned, "
                f"{cleaned} memory files cleaned | "
                f"Disk: logs={stats['logs_mb']}MB, mem={stats['memory_mb']}MB, db={stats['db_mb']}MB"
            )

        except Exception as e:
            log.error(f"Log janitor error: {e}")
            update_agent_status(AGENT_NAME, status="error", error=str(e))

        time.sleep(INTERVAL)

    log.info("Log janitor stopped")


if __name__ == "__main__":
    main()
