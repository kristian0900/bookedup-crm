"""
BookedUp Dashboard Server
FastAPI dashboard — dark charcoal, burnt orange accents, Playfair Display + DM Sans.
"""

import sys
import os
import time
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Request, Form, Cookie
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import hashlib
import secrets

from shared.db import get_all_agent_statuses, peek_events, get_connection

ACE_PIN = "JR1234"
ACE_SESSION_SECRET = secrets.token_hex(16)


def make_session_token(pin):
    return hashlib.sha256(f"{pin}:{ACE_SESSION_SECRET}".encode()).hexdigest()


ACE_VALID_TOKEN = make_session_token(ACE_PIN)

app = FastAPI(title="BookedUp Dashboard")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))


def safe_query(conn, sql, params=()):
    try:
        return conn.execute(sql, params).fetchall()
    except Exception:
        return []


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    agents = get_all_agent_statuses()
    now = time.time()

    for a in agents:
        if a.get("last_heartbeat"):
            ago = int(now - a["last_heartbeat"])
            if ago < 60:
                a["ago_str"] = f"{ago}s ago"
            elif ago < 3600:
                a["ago_str"] = f"{ago // 60}m ago"
            else:
                a["ago_str"] = f"{ago // 3600}h ago"
            a["ago"] = ago
        else:
            a["ago"] = None
            a["ago_str"] = "never"

    HIDDEN_SOURCES = {"heartbeat", "log_janitor"}
    all_events = peek_events(limit=100)
    recent_events = []
    for e in all_events:
        if e.get("source") in HIDDEN_SOURCES:
            continue
        e["time_str"] = datetime.fromtimestamp(e["timestamp"]).strftime("%H:%M:%S")
        try:
            payload = json.loads(e["payload"]) if isinstance(e["payload"], str) else e["payload"]
            parts = []
            for key in ["agent", "work_order_id", "customer", "name", "reminder_type", "field"]:
                if key in payload:
                    parts.append(f"{key}={payload[key]}")
            e["detail"] = ", ".join(parts[:3]) if parts else ""
        except Exception:
            e["detail"] = ""
        recent_events.append(e)
        if len(recent_events) >= 40:
            break

    conn = get_connection()

    # Work order stats
    wo_stats = {}
    for status in ["new", "scheduled", "in_progress", "on_hold", "completed", "cancelled"]:
        row = conn.execute(
            "SELECT COUNT(*) as c FROM work_orders WHERE status = ?", (status,)
        ).fetchone()
        wo_stats[status] = row["c"] if row else 0
    total_wo = sum(wo_stats.values())

    # Recent work orders
    work_orders = [dict(r) for r in safe_query(
        conn,
        "SELECT id, customer_name, address, description, status, priority, assigned_to, created_at "
        "FROM work_orders ORDER BY created_at DESC LIMIT 15"
    )]
    for wo in work_orders:
        wo["created_str"] = datetime.fromtimestamp(wo["created_at"]).strftime("%m/%d %I:%M%p")

    # Drip sequence descriptions
    DRIP_STEPS = [
        "Initial outreach — intro message + value prop",
        "Follow-up — share a quick win or case study",
        "Value add — send relevant tip or industry insight",
        "Check-in — ask if they have questions, offer demo",
        "Social proof — share testimonial or results",
        "Final push — limited-time offer or personal invite",
        "Long-term nurture — monthly check-in begins",
    ]

    # Prospects
    prospects = [dict(r) for r in safe_query(
        conn,
        "SELECT id, name, business_name, phone, email, industry, location, status, "
        "current_step, added_at, last_contact_at, next_touch_at, notes, tags "
        "FROM prospects WHERE status = 'active' ORDER BY next_touch_at ASC LIMIT 25"
    )]
    for p in prospects:
        step = p["current_step"]
        p["step_display"] = f"{step + 1}/7"
        p["step_action"] = DRIP_STEPS[step] if step < len(DRIP_STEPS) else "Monthly check-in"
        if p.get("next_touch_at"):
            p["next_touch_str"] = datetime.fromtimestamp(p["next_touch_at"]).strftime("%b %d, %I:%M %p")
            if p["next_touch_at"] < now:
                days_over = int((now - p["next_touch_at"]) / 86400)
                p["overdue"] = days_over
            else:
                p["overdue"] = -1
        else:
            p["next_touch_str"] = "Not set"
            p["overdue"] = -1
        p["days_in"] = int((now - p["added_at"]) / 86400)
        if p.get("last_contact_at"):
            p["last_contact_str"] = datetime.fromtimestamp(p["last_contact_at"]).strftime("%b %d, %I:%M %p")
        else:
            p["last_contact_str"] = "Never"

    prospect_count = len(prospects)

    # Bookings
    bookings = [dict(r) for r in safe_query(
        conn,
        "SELECT id, title, attendee_name, attendee_email, start_time, status "
        "FROM bookings ORDER BY start_time ASC LIMIT 10"
    )]

    conn.close()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "agents": agents,
        "events": recent_events,
        "wo_stats": wo_stats,
        "total_wo": total_wo,
        "work_orders": work_orders,
        "prospects": prospects,
        "prospect_count": prospect_count,
        "bookings": bookings,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


@app.get("/new-job", response_class=HTMLResponse)
async def new_job_form(request: Request, created: int = 0):
    return templates.TemplateResponse("new_job.html", {
        "request": request,
        "created_id": created,
    })


@app.post("/new-job", response_class=HTMLResponse)
async def new_job_submit(
    request: Request,
    customer_name: str = Form(...),
    phone: str = Form(""),
    address: str = Form(""),
    description: str = Form(...),
    priority: str = Form("normal"),
):
    conn = get_connection()
    notes = f"Phone: {phone}" if phone else ""
    cursor = conn.execute(
        "INSERT INTO work_orders (created_at, customer_name, address, description, status, priority, assigned_to, notes) "
        "VALUES (?, ?, ?, ?, 'new', ?, 'JR', ?)",
        (time.time(), customer_name.strip(), address.strip(), description.strip(), priority, notes)
    )
    wo_id = cursor.lastrowid
    conn.commit()
    conn.close()

    from shared.db import publish_event
    publish_event("intake_agent", "work_order_created", {
        "work_order_id": wo_id,
        "customer": customer_name.strip(),
        "address": address.strip(),
        "description": description.strip(),
        "source": "web_form",
    })

    return templates.TemplateResponse("new_job.html", {
        "request": request,
        "created_id": wo_id,
    })


@app.get("/ace", response_class=HTMLResponse)
async def ace_dashboard(request: Request):
    conn = get_connection()
    now = time.time()

    # Active work orders: new, scheduled, in_progress only
    active_orders = [dict(r) for r in safe_query(
        conn,
        "SELECT * FROM work_orders WHERE status IN ('new', 'scheduled', 'in_progress') "
        "ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'urgent' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END, created_at DESC"
    )]
    status_labels = {
        "new": "New", "scheduled": "Scheduled", "in_progress": "In Progress",
        "on_hold": "On Hold", "completed": "Completed", "cancelled": "Cancelled",
    }
    for wo in active_orders:
        wo["created_str"] = datetime.fromtimestamp(wo["created_at"]).strftime("%m/%d %I:%M %p")
        wo["status_display"] = status_labels.get(wo["status"], wo["status"])

    # Completed work orders (last 7 days)
    week_ago = now - (7 * 86400)
    completed_orders = [dict(r) for r in safe_query(
        conn,
        "SELECT * FROM work_orders WHERE status = 'completed' AND completed_at > ? "
        "ORDER BY completed_at DESC LIMIT 20", (week_ago,)
    )]
    for wo in completed_orders:
        if wo.get("completed_at"):
            wo["completed_str"] = datetime.fromtimestamp(wo["completed_at"]).strftime("%m/%d %I:%M %p")
        else:
            wo["completed_str"] = ""
        wo["created_str"] = datetime.fromtimestamp(wo["created_at"]).strftime("%m/%d %I:%M %p")
        wo["status_display"] = status_labels.get(wo["status"], wo["status"])

    # Counts
    today_start = time.mktime(datetime.now().replace(hour=0, minute=0, second=0).timetuple())
    completed_today_row = conn.execute(
        "SELECT COUNT(*) as c FROM work_orders WHERE status = 'completed' AND completed_at > ?",
        (today_start,)
    ).fetchone()
    completed_today = completed_today_row["c"] if completed_today_row else 0

    completed_week_row = conn.execute(
        "SELECT COUNT(*) as c FROM work_orders WHERE status = 'completed' AND completed_at > ?",
        (week_ago,)
    ).fetchone()
    completed_week = completed_week_row["c"] if completed_week_row else 0

    total_row = conn.execute("SELECT COUNT(*) as c FROM work_orders").fetchone()
    total_wo = total_row["c"] if total_row else 0

    # Status flow counts
    flow_steps = []
    for status, label in [("new", "New"), ("scheduled", "Scheduled"),
                           ("in_progress", "In Progress"), ("on_hold", "On Hold"),
                           ("completed", "Completed")]:
        row = conn.execute(
            "SELECT COUNT(*) as c FROM work_orders WHERE status = ?", (status,)
        ).fetchone()
        flow_steps.append({"status": status, "label": label, "count": row["c"] if row else 0})

    conn.close()

    active_count = sum(1 for wo in active_orders)
    now_dt = datetime.now()
    from datetime import timedelta
    tomorrow = (now_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    return templates.TemplateResponse("ace_dashboard.html", {
        "request": request,
        "active_orders": active_orders,
        "completed_orders": completed_orders,
        "active_count": active_count,
        "completed_today": completed_today,
        "completed_week": completed_week,
        "total_wo": total_wo,
        "flow_steps": flow_steps,
        "date_display": now_dt.strftime("%A, %B %d"),
        "time_display": now_dt.strftime("%I:%M %p"),
        "tomorrow": tomorrow,
    })


@app.get("/api/status")
async def api_status():
    return {"agents": get_all_agent_statuses(), "timestamp": time.time()}


@app.post("/api/wo/{wo_id}/status/{new_status}")
async def update_wo_status(wo_id: int, new_status: str):
    valid = {"new", "scheduled", "in_progress", "on_hold", "completed"}
    if new_status not in valid:
        return {"error": "Invalid status"}
    conn = get_connection()
    row = conn.execute("SELECT status FROM work_orders WHERE id = ?", (wo_id,)).fetchone()
    if not row:
        conn.close()
        return {"error": "Not found"}
    updates = {"status": new_status, "updated_at": time.time()}
    if new_status == "completed":
        updates["completed_at"] = time.time()
    elif row["status"] == "completed":
        updates["completed_at"] = None
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    conn.execute(f"UPDATE work_orders SET {set_clause} WHERE id = ?", list(updates.values()) + [wo_id])
    conn.commit()
    conn.close()
    from shared.db import publish_event
    publish_event("status_updater", "work_order_updated", {
        "work_order_id": wo_id, "field": "status",
        "old_value": row["status"], "new_value": new_status,
    })
    return {"ok": True, "new_status": new_status}


@app.post("/api/wo/{wo_id}/schedule")
async def schedule_wo(wo_id: int, request: Request):
    body = await request.json()
    sched_date = body.get("date", "")
    sched_time = body.get("time_slot", "")
    sched_notes = body.get("notes", "")

    scheduled_for = f"{sched_date} {sched_time}".strip()

    conn = get_connection()
    row = conn.execute("SELECT status, customer_name FROM work_orders WHERE id = ?", (wo_id,)).fetchone()
    if not row:
        conn.close()
        return {"error": "Not found"}

    note_entry = ""
    if sched_notes:
        from datetime import datetime as dt
        ts = dt.now().strftime("%m/%d %H:%M")
        note_entry = f"\n[{ts}] Scheduled: {sched_notes}"

    conn.execute(
        "UPDATE work_orders SET status = 'scheduled', scheduled_for = ?, updated_at = ?, "
        "notes = COALESCE(notes, '') || ? WHERE id = ?",
        (scheduled_for, time.time(), note_entry, wo_id)
    )
    conn.commit()
    conn.close()

    from shared.db import publish_event
    publish_event("status_updater", "work_order_scheduled", {
        "work_order_id": wo_id,
        "customer": row["customer_name"],
        "scheduled_for": scheduled_for,
    })

    # Send Telegram notification
    from shared.notify import send_telegram
    send_telegram(
        f"<b>Job #{wo_id} scheduled</b>\n"
        f"{sched_date} — {sched_time}\n"
        f"Customer: {row['customer_name']}"
        f"{chr(10) + 'Note: ' + sched_notes if sched_notes else ''}"
    )

    return {"ok": True, "scheduled_for": scheduled_for}


@app.post("/api/prospect/{prospect_id}/contacted")
async def prospect_contacted(prospect_id: int):
    """Mark current step complete and advance to next."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM prospects WHERE id = ?", (prospect_id,)).fetchone()
    if not row:
        conn.close()
        return {"error": "Not found"}
    new_step = row["current_step"] + 1
    # Calculate next touch
    DRIP_DAYS = [0, 3, 7, 14, 21, 30, 45]
    if new_step < len(DRIP_DAYS):
        next_touch = row["added_at"] + (DRIP_DAYS[new_step] * 86400)
    else:
        next_touch = time.time() + (30 * 86400)
    conn.execute(
        "UPDATE prospects SET current_step = ?, last_contact_at = ?, next_touch_at = ? WHERE id = ?",
        (new_step, time.time(), next_touch, prospect_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True, "new_step": new_step}


@app.post("/api/prospect/{prospect_id}/skip")
async def prospect_skip(prospect_id: int):
    """Skip current step without marking as contacted."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM prospects WHERE id = ?", (prospect_id,)).fetchone()
    if not row:
        conn.close()
        return {"error": "Not found"}
    new_step = row["current_step"] + 1
    DRIP_DAYS = [0, 3, 7, 14, 21, 30, 45]
    if new_step < len(DRIP_DAYS):
        next_touch = row["added_at"] + (DRIP_DAYS[new_step] * 86400)
    else:
        next_touch = time.time() + (30 * 86400)
    conn.execute(
        "UPDATE prospects SET current_step = ?, next_touch_at = ? WHERE id = ?",
        (new_step, next_touch, prospect_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True, "new_step": new_step}


@app.post("/api/prospect/{prospect_id}/remove")
async def prospect_remove(prospect_id: int):
    """Deactivate a prospect."""
    conn = get_connection()
    conn.execute("UPDATE prospects SET status = 'removed' WHERE id = ?", (prospect_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    import threading

    # Main ops dashboard on 8080
    # Ace client dashboard also served from 8080/ace
    # Standalone Ace on 8081
    ace_app = FastAPI(title="Ace Handyman Dashboard")
    ace_templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

    def ace_is_logged_in(request: Request):
        token = request.cookies.get("ace_session")
        return token == ACE_VALID_TOKEN

    @ace_app.get("/login", response_class=HTMLResponse)
    async def ace_login_page(request: Request, error: str = ""):
        return templates.TemplateResponse("ace_login.html", {"request": request, "error": error})

    @ace_app.post("/login")
    async def ace_login_submit(request: Request, pin: str = Form(...)):
        if pin == ACE_PIN:
            response = RedirectResponse(url="/", status_code=303)
            response.set_cookie("ace_session", ACE_VALID_TOKEN, httponly=True, samesite="lax")
            return response
        return templates.TemplateResponse("ace_login.html", {"request": request, "error": "Wrong PIN. Try again."})

    @ace_app.post("/api/wo/{wo_id}/status/{new_status}")
    async def ace_update_wo(wo_id: int, new_status: str, request: Request):
        if not ace_is_logged_in(request):
            return {"error": "Not logged in"}
        return await update_wo_status(wo_id, new_status)

    @ace_app.post("/api/wo/{wo_id}/schedule")
    async def ace_schedule_wo(wo_id: int, request: Request):
        if not ace_is_logged_in(request):
            return {"error": "Not logged in"}
        return await schedule_wo(wo_id, request)

    @ace_app.get("/logout")
    async def ace_logout():
        response = RedirectResponse(url="/login", status_code=303)
        response.delete_cookie("ace_session")
        return response

    @ace_app.get("/", response_class=HTMLResponse)
    async def ace_standalone(request: Request):
        if not ace_is_logged_in(request):
            return RedirectResponse(url="/login", status_code=303)
        return await ace_dashboard(request)

    @ace_app.get("/new-job", response_class=HTMLResponse)
    async def ace_new_job_form(request: Request, created: int = 0):
        if not ace_is_logged_in(request):
            return RedirectResponse(url="/login", status_code=303)
        return await new_job_form(request, created)

    @ace_app.post("/new-job", response_class=HTMLResponse)
    async def ace_new_job_submit(
        request: Request,
        customer_name: str = Form(...),
        phone: str = Form(""),
        address: str = Form(""),
        description: str = Form(...),
        priority: str = Form("normal"),
    ):
        if not ace_is_logged_in(request):
            return RedirectResponse(url="/login", status_code=303)
        return await new_job_submit(request, customer_name, phone, address, description, priority)

    def run_ace():
        uvicorn.run(ace_app, host="0.0.0.0", port=8081, log_level="warning")

    t = threading.Thread(target=run_ace, daemon=True)
    t.start()

    uvicorn.run(app, host="0.0.0.0", port=8080)
