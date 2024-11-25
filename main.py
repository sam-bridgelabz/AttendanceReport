import json
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from zoneinfo import ZoneInfo

import gspread
import httpx
import pandas as pd
import redis
from apscheduler.schedulers.blocking import BlockingScheduler
from fastapi import APIRouter, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from google.oauth2.service_account import Credentials
from pydantic import BaseModel, EmailStr

cache = redis.StrictRedis(**{"host": "localhost", "port": 6379, "db": 0, "password": "Zeus.1996"})
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
    # print(data)
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
scheduler.add_job(update_master_email_cache, "cron", hour=0, minute=30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=lambda: scheduler.start())
    thread.daemon = True
    thread.start()
    yield


templates = Jinja2Templates(directory="templates")

router = APIRouter(prefix="/api")

app = FastAPI(title="Attendance", lifespan=lifespan, docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory="static"), name="static")
# app.include_router(router)


class Actions(Enum):
    login = "Login"
    logout = "Logout"


class FormModel(BaseModel):
    email: EmailStr
    action: Actions
    ip: str


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


def form_submit(data):
    ip = data.get("ip")
    if ip not in IP:
        return "You are not connected to proper network to use this feature", "error"

    email = data.get("email")
    action = data.get("action")
    if not email or not action:
        return "Invalid form data", "error"

    user_log = cache.get(name="user_log")
    user_log = json.loads(user_log) if user_log else {}

    candidate_details = user_log.get(email)

    utc_time = datetime.now(tz=timezone.utc)
    log_time = utc_time.astimezone(tz=ZoneInfo(key="Asia/Kolkata")).strftime("%m/%d/%Y %H:%M:%S")

    if candidate_details and action == "Login":
        return "Login already done for today", "error"
    elif candidate_details and action == "Logout":
        if "Logout" in candidate_details:
            return "Logout already done for today", "error"
        candidate_details.update({action: log_time})
    elif not candidate_details and action == "Logout":
        return "Login not done yet for today", "error"
    elif action == "Login":
        if not candidate_details:
            candidate_details = {}
        candidate_details.update({action: log_time})
    candidate_details.update({"email": email, "ip": ip})
    user_log.update({email: candidate_details})
    cache.set(name="user_log", value=json.dumps(user_log))
    return f"{action} successful", "success"


@app.api_route("/", methods=["GET", "POST"], response_class=HTMLResponse)
async def render_portal(request: Request, response: Response):
    message = "Login successful"
    msg_type = "success"
    disallowed_browsers = [
        "firefox",
        # "safari",
        "opr/",
        "edg/",
        "vivaldi",
        "brave",
        "duckduckgo",
    ]

    user_agent = request.headers.get("user-agent", "").lower()

    if any(device in user_agent for device in ["mobi", "android", "iphone", "ipad", "ipod"]):
        # raise HTTPException(status_code=403, detail="Mobile devices are not supported.")
        message = "Mobile devices are not supported."
        msg_type = "error"
        return templates.TemplateResponse(
            "portal.html", {"request": request, "message": message, "msg_type": msg_type}
        )

    if "chrome" not in user_agent or any(browser in user_agent for browser in disallowed_browsers):
        # raise HTTPException(
        #     status_code=403,
        #     detail="This application is only accessible via Google Chrome. Please switch to Chrome.",
        # )
        message = "This application is only accessible via Google Chrome. Please switch to Chrome."
        msg_type = "error"
        return templates.TemplateResponse(
            "portal.html", {"request": request, "message": message, "msg_type": msg_type}
        )

    cookie_action = request.cookies.get("next_action", "Login")
    cookie_for = request.cookies.get("for", "")

    if request.method == "POST":
        form_data = await request.form()
        data = dict(form_data.items())

        message, msg_type = form_submit(data=data)

        max_age = set_cookie_age()

        response.set_cookie(
            key="next_action",
            value=cookie_action,
            httponly=True,
            max_age=max_age,
            domain=None,
            samesite="lax",
            secure=False,
        )

        response.set_cookie(
            key="for",
            value=cookie_for,
            path="/",
            httponly=True,
            max_age=max_age,
            domain=None,
            samesite="lax",
            secure=False,
        )

    return templates.TemplateResponse(
        "portal.html",
        {
            "request": request,
            "action": cookie_action,
            "for": cookie_for,
            "message": "",
            "msg_type": "success",
        },
    )


# @router.post("/submit", status_code=status.HTTP_200_OK)
# def form_submission(request: Request, response: Response, body: FormModel):
#     ip = body.ip
#     # if ip not in IP:
#     #     response.status_code = status.HTTP_406_NOT_ACCEPTABLE
#     #     return {
#     #         "message": "Looks like you are not connected to proper network to use this feature",
#     #         "status": "error",
#     #     }
#     data = body.model_dump()
#     email = data.get("email")
#     action = data.get("action")
#     if not email:
#         response.status_code = status.HTTP_404_NOT_FOUND
#         return {"message": "Email not found", "status": "error"}
#     if not action:
#         response.status_code = status.HTTP_404_NOT_FOUND
#         return {"message": "Action not found", "status": "error"}
#     action = action.value

#     user_log = cache.get(name="user_log")
#     if user_log:
#         user_log = json.loads(user_log)
#     else:
#         user_log = {}

#     candidate_details = user_log.get(email)
#     # if not candidate_details:
#     #     candidate_details = {"email": email, "ip": ip}

#     utc_time = datetime.now(tz=timezone.utc)
#     log_time = utc_time.astimezone(tz=ZoneInfo(key="Asia/Kolkata")).strftime("%m/%d/%Y %H:%M:%S")
#     if candidate_details and action == "Login":
#         response.status_code = status.HTTP_406_NOT_ACCEPTABLE
#         return {"message": "Login already done for today", "status": "error"}
#     elif candidate_details and action == "Logout":
#         # candidate_details = candidate_details
#         if "Logout" in candidate_details:
#             response.status_code = status.HTTP_406_NOT_ACCEPTABLE
#             return {"message": "Logout already done for today", "status": "error"}
#         candidate_details.update({action: log_time})
#     elif not candidate_details and action == "Logout":
#         response.status_code = status.HTTP_406_NOT_ACCEPTABLE
#         return {"message": "Login not done yet for today", "status": "error"}
#     elif action == "Login":
#         if not candidate_details:
#             candidate_details = {}
#         candidate_details.update({action: log_time})
#     candidate_details.update({"email": email, "ip": ip})
#     user_log.update({email: candidate_details})
#     cache.set(name="user_log", value=json.dumps(user_log))
#     response.set_cookie(
#         key="action",
#         value=action,
#         path="/",
#         httponly=True,
#         max_age=set_cookie_age(),
#         domain=None,
#         samesite="lax",
#         secure=False,
#     )
#     return {
#         "message": f"{action} Successful",
#         "status": "success",
#         "data": candidate_details,
#     }


@router.get("/user/{email}", status_code=status.HTTP_200_OK)
def get_user_details(request: Request, response: Response, email: str):
    master_email_lst = cache.get(name=MASTER_EMAIL_CACHE_KEY)
    if not master_email_lst:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"message": "Unable to fetch details... Try again later", "status": "error"}

    master_email_lst = json.loads(master_email_lst)
    if email not in master_email_lst:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "Sorry... You are not an active candidate!", "status": "error"}

    user_log = cache.get("user_log")
    if not user_log:
        user_log = {}
    else:
        user_log = json.loads(user_log)

    is_exist = user_log.get(email)
    action, api_status = "", "success"

    if not is_exist:
        action = "Login"
    else:
        if "Login" and "Logout" in is_exist:
            action = "All done for today"
            api_status = "warning"
        elif "Logout" not in is_exist:
            action = "Logout"
    return {
        "message": action,
        "status": api_status,
    }


app.include_router(router)
