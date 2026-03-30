"""
Telegram notification module for BookedUp agent system.
"""

import requests
import yaml
import os

CONFIG_PATH = "/Users/bookedup/bookedup/config.yaml"


def _load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def send_telegram(message, parse_mode="HTML"):
    cfg = _load_config()
    token = cfg["telegram"]["bot_token"]
    chat_id = cfg["telegram"]["chat_id"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        # Log but don't crash the agent
        print(f"[notify] Telegram send failed: {e}")
        return None


def send_alert(agent_name, message):
    text = f"<b>[{agent_name}]</b>\n{message}"
    return send_telegram(text)


def send_error(agent_name, error):
    text = f"<b>[{agent_name}] ERROR</b>\n<code>{error}</code>"
    return send_telegram(text)


def send_daily_digest(summary):
    text = f"<b>Daily Digest</b>\n\n{summary}"
    return send_telegram(text)
