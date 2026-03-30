"""
BookedUp Agent Launcher
Starts each agent as its own subprocess and monitors them.
Uses a PID file to prevent multiple launchers, and kills stale agents on startup.
"""

import subprocess
import sys
import os
import time
import signal
import yaml
import atexit

BASE_DIR = "/Users/bookedup/bookedup"
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")
PID_FILE = os.path.join(BASE_DIR, "launcher.pid")

AGENT_SCRIPTS = {
    "telegram_poller": "shared/telegram_poller.py",
    "booking_watcher": "agents/booking_watcher.py",
    "prospect_nurture": "agents/prospect_nurture.py",
    "intake_agent": "agents/intake_agent.py",
    "status_updater": "agents/status_updater.py",
    "reminder_agent": "agents/reminder_agent.py",
    "daily_digest": "agents/daily_digest.py",
    "log_janitor": "agents/log_janitor.py",
    "heartbeat": "agents/heartbeat.py",
}

processes = {}
running = True


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def kill_stale_agents():
    """Kill any leftover BookedUp agent processes from previous runs."""
    import re
    try:
        result = subprocess.run(
            ["ps", "aux"], capture_output=True, text=True
        )
        for line in result.stdout.splitlines():
            if "Python" in line and "bookedup/" in line and str(os.getpid()) not in line:
                parts = line.split()
                pid = int(parts[1])
                try:
                    os.kill(pid, signal.SIGKILL)
                    print(f"[launcher] Killed stale process PID {pid}")
                except ProcessLookupError:
                    pass
    except Exception as e:
        print(f"[launcher] Warning: couldn't clean stale processes: {e}")


def check_pid_file():
    """Ensure no other launcher is running."""
    if os.path.exists(PID_FILE):
        with open(PID_FILE, "r") as f:
            old_pid = int(f.read().strip())
        try:
            os.kill(old_pid, 0)  # Check if process exists
            print(f"[launcher] Another launcher is running (PID {old_pid}), killing it...")
            os.kill(old_pid, signal.SIGKILL)
            time.sleep(1)
        except ProcessLookupError:
            pass  # Already dead

    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))


def cleanup_pid_file():
    try:
        os.remove(PID_FILE)
    except FileNotFoundError:
        pass


def start_agent(name, script):
    script_path = os.path.join(BASE_DIR, script)
    if not os.path.exists(script_path):
        print(f"[launcher] Script not found: {script_path}, skipping {name}")
        return None

    proc = subprocess.Popen(
        [sys.executable, "-B", script_path],
        cwd=BASE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    print(f"[launcher] Started {name} (PID {proc.pid})")
    return proc


def kill_all_children():
    """Force kill all child processes."""
    for name, proc in processes.items():
        if proc and proc.poll() is None:
            try:
                proc.kill()
                print(f"[launcher] Killed {name} (PID {proc.pid})")
            except Exception:
                pass


def shutdown(signum, frame):
    global running
    print(f"\n[launcher] Received signal {signum}, shutting down all agents...")
    running = False
    kill_all_children()
    cleanup_pid_file()
    sys.exit(0)


def main():
    global running

    # Prevent duplicate launchers
    check_pid_file()
    atexit.register(cleanup_pid_file)
    atexit.register(kill_all_children)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Kill any orphaned agents from previous runs
    kill_stale_agents()
    time.sleep(1)

    config = load_config()
    agent_config = config.get("agents", {})

    print("[launcher] BookedUp Agent System starting...")
    print(f"[launcher] Base directory: {BASE_DIR}")
    print(f"[launcher] Database: {config['database']['path']}")
    print()

    for name, script in AGENT_SCRIPTS.items():
        agent_cfg = agent_config.get(name, {})
        if not agent_cfg.get("enabled", True):
            print(f"[launcher] {name} is disabled, skipping")
            continue
        proc = start_agent(name, script)
        if proc:
            processes[name] = proc

    print(f"\n[launcher] {len(processes)} agents running. Press Ctrl+C to stop.\n")

    while running:
        for name, proc in list(processes.items()):
            if proc.poll() is not None:
                print(f"[launcher] {name} exited with code {proc.returncode}, restarting...")
                time.sleep(2)
                new_proc = start_agent(name, AGENT_SCRIPTS[name])
                if new_proc:
                    processes[name] = new_proc
        time.sleep(5)


if __name__ == "__main__":
    main()
