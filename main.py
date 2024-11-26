import json
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import gspread
import httpx
import pandas as pd
import redis
from apscheduler.schedulers.blocking import BlockingScheduler
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from google.oauth2.service_account import Credentials

IP = ["106.51.84.193"]
MASTER_EMAIL_CACHE_KEY = "master_emails"


def flush_redis():
    cache.flushall()


def update_master_email_cache():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://spreadsheets.google.com/feeds",
    ]
    is_auth = Credentials.from_service_account_file("login_cred.json", scopes=scopes)
    gc = gspread.authorize(is_auth)

    # Open google sheet
    master_sheet = gc.open("Offline Attendance @Blr")

    # Get worksheet
    master_worksheet = master_sheet.worksheet("Nov-24")

    data = master_worksheet.get_all_values()

    master_df = pd.DataFrame(data[1:], columns=data[0])

    # Fetches only active candidates from master sheet
    cols = ["Admit ID", "Email", "Blr LabStatus"]
    master_df = master_df[cols]
    master_df = master_df[master_df["Blr LabStatus"] == "Active"]
    email_lst = master_df["Email"].to_list()
    email_lst = json.dumps(email_lst)
    cache.set(name=MASTER_EMAIL_CACHE_KEY, value=email_lst)


scheduler = BlockingScheduler()
scheduler.add_job(flush_redis, "cron", minute=0, hour=0)
scheduler.add_job(update_master_email_cache, "cron", minute=0, hour=0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=lambda: scheduler.start())
    thread.daemon = True
    thread.start()
    yield


templates = Jinja2Templates(directory="templates")

app = FastAPI(title="Attendance", lifespan=lifespan, docs_url=None, redoc_url=None)

cache = redis.StrictRedis(**{"host": "localhost", "port": 6379, "db": 0, "password": "Zeus.1996"})

disallowed_browsers = [
    "firefox",
    # "safari",
    "opr/",
    "edg/",
    "vivaldi",
    "brave",
    "duckduckgo",
]


def get_local_ipv4():
    try:
        ip_resp = httpx.get(url="https://api.ipify.org?format=json")
    except httpx.ConnectError as e:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT, detail="Unable to detect ip address"
        ) from e
    except httpx.ConnectTimeout as e:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT, detail="Unable to detect ip address"
        ) from e
    if ip_resp.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Unable to detect ip address"
        )
    ip_address = ip_resp.json().get("ip")
    if not ip_address:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Unable to detect ip address"
        )
    return ip_address


def set_cookie_age():
    utc_time = datetime.now(tz=timezone.utc)
    log_time = utc_time.astimezone(tz=ZoneInfo(key="Asia/Kolkata"))
    exp_time = datetime(
        year=log_time.year,
        month=log_time.month,
        day=log_time.day,
        # hour=log_time.hour,
        # minute=log_time.minute,
        tzinfo=ZoneInfo("Asia/Kolkata"),
    ) + timedelta(days=1)
    exp_seconds = (exp_time - log_time).total_seconds()
    return int(abs(exp_seconds))


def submit_form(data):
    ip = data.get("ip")
    if ip not in IP:
        return "You are not connected to proper network to use this feature", "warning"

    email = data.get("email")
    action = data.get("action")
    if not email or not action:
        return "Invalid form data", "warning"

    master_lst = cache.get("master_emails")
    master_lst = json.loads(master_lst) if master_lst else []

    if email not in master_lst:
        return "Sorry... You are not an active user", "warning"

    user_log = cache.get(name="user_log")
    user_log = json.loads(user_log) if user_log else {}
    candidate_details = user_log.get(email, {})
    utc_time = datetime.now(tz=timezone.utc)
    log_time = utc_time.astimezone(tz=ZoneInfo(key="Asia/Kolkata")).strftime("%m/%d/%Y %H:%M:%S")

    if candidate_details and action == "Login":
        return "Login already done for today", "warning"
    elif candidate_details and action == "Logout":
        if "Logout" in candidate_details:
            return "Logout already done for today", "warning"
        candidate_details.update({action: log_time})
    elif not candidate_details and action == "Logout":
        return "Login not done yet for today", "warning"
    elif action == "Login":
        candidate_details.update({action: log_time})
    candidate_details.update({"email": email, "ip": ip})
    user_log.update({email: candidate_details})
    cache.set(name="user_log", value=json.dumps(user_log))
    return f"{action} successful", "success"


@app.api_route("/", methods=["GET", "POST"], response_class=HTMLResponse)
async def render_portal(request: Request):
    message = ""
    msg_type = "success"

    user_agent = request.headers.get("user-agent", "").lower()

    if any(device in user_agent for device in ["mobi", "android", "iphone", "ipad", "ipod"]):
        message = "Mobile devices are not supported."
        msg_type = "error"
        return templates.TemplateResponse(
            "portal.html", {"request": request, "message": message, "msg_type": msg_type}
        )

    if "chrome" not in user_agent or any(browser in user_agent for browser in disallowed_browsers):
        message = "This application is only accessible via Google Chrome. Please switch to Chrome."
        msg_type = "error"
        return templates.TemplateResponse(
            "portal.html", {"request": request, "message": message, "msg_type": msg_type}
        )

    cookie_action = request.cookies.get("action", "Login")
    cookie_for = request.cookies.get("for", "")

    if request.method == "POST":
        form_data = await request.form()
        data = dict(form_data.items())

        message, msg_type = submit_form(data=data)

        max_age = set_cookie_age()

        cookie_action = "Logout" if cookie_action == "Login" else "All done for today"

        response = RedirectResponse(url="/", status_code=303)

        if msg_type == "success":
            response.set_cookie(
                key="action",
                value=cookie_action,
                path="/",
                max_age=max_age,
                httponly=True,
                samesite="lax",
            )

            response.set_cookie(
                key="for",
                value=data.get("email"),
                path="/",
                max_age=max_age,
                httponly=True,
                samesite="lax",
            )

        response.set_cookie(
            key="message",
            value=message,
            path="/",
            max_age=max_age,
            httponly=True,
            samesite="lax",
        )

        response.set_cookie(
            key="msg_type",
            value=msg_type,
            path="/",
            max_age=max_age,
            httponly=True,
            samesite="lax",
        )

        return response

    message = request.cookies.get("message", "")
    msg_type = request.cookies.get("msg_type", "success")

    response = templates.TemplateResponse(
        name="portal.html",
        context={
            "request": request,
            "action": cookie_action,
            "for": cookie_for,
            "message": message,
            "msg_type": msg_type,
        },
    )

    response.delete_cookie("message")
    response.delete_cookie("msg_type")

    return response
