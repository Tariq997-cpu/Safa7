"""
Safa7 — WhatsApp AI Assistant
Flask + Twilio + Anthropic Claude + Google Sheets + yfinance

Priority: Task management, team tracking, memory recall, reminders.
"""

import os
import json
import time
import threading
import traceback
import uuid
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, request, abort
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client as TwilioClient
from twilio.request_validator import RequestValidator
import anthropic


# ━━━ Configuration ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app = Flask(__name__)

MODEL = "claude-sonnet-4-20250514"
MAX_HISTORY = 50
MAX_MSG_LEN = 1600
MAX_TOKENS  = 2048
AST = timezone(timedelta(hours=3))

YF_TICKERS = {
    "brent": "BZ=F",
    "wti": "CL=F",
    "eur/usd": "EURUSD=X",
    "gbp/usd": "GBPUSD=X",
    "s&p 500": "^GSPC",
    "nasdaq": "^IXIC",
}

YF_TRIGGERS = list(YF_TICKERS.keys())

SAUDI_TRIGGERS = [
    "tasi", "tadawul", "aramco", "sabic", "stc", "alrajhi", "al rajhi",
    "riyad bank", "alinma", "saudi stock", "saudi market", "saudi exchange",
    "usd/sar", "usd sar", "dollar sar", "ريال", "تاسي", "تداول", "أرامكو",
]


# ━━━ Clients ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

claude = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

TWILIO_SID    = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN")
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "whatsapp:+14155238886")
OWNER_NUMBER  = os.environ.get("OWNER_NUMBER", "whatsapp:+966553424848")
twilio_client    = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
twilio_validator = RequestValidator(TWILIO_TOKEN)

_GS_CREDS  = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
_GS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID   = os.environ.get("GOOGLE_SHEET_ID")


# ━━━ Thread Safety ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_history_lock       = threading.Lock()
_profile_lock       = threading.Lock()
_reminder_lock      = threading.Lock()
_sender_locks: dict = {}
_sender_locks_meta  = threading.Lock()
_executor           = ThreadPoolExecutor(max_workers=4)
_gs_lock            = threading.Lock()


def _get_sender_lock(sender: str) -> threading.Lock:
    with _sender_locks_meta:
        if sender not in _sender_locks:
            _sender_locks[sender] = threading.Lock()
        return _sender_locks[sender]


# ━━━ Google Sheets ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gs():
    with _gs_lock:
        creds = Credentials.from_service_account_info(_GS_CREDS, scopes=_GS_SCOPES)
        return gspread.authorize(creds)


def _get_worksheet(tab: int):
    return _gs().open_by_key(SHEET_ID).get_worksheet(tab)


def _get_worksheet_by_name(name: str):
    return _gs().open_by_key(SHEET_ID).worksheet(name)


def _init_sheets():
    if not _GS_CREDS or not SHEET_ID:
        return
    try:
        spreadsheet = _gs().open_by_key(SHEET_ID)
        titles = [ws.title for ws in spreadsheet.worksheets()]
        if "Profile" not in titles:
            ws = spreadsheet.add_worksheet(title="Profile", rows=200, cols=3)
            ws.update(values=[["key", "value", "updated"]], range_name="A1:C1")
            print("[INIT] Profile tab created")
        if "Reminders" not in titles:
            ws = spreadsheet.add_worksheet(title="Reminders", rows=200, cols=5)
            ws.update(values=[["id", "message", "due", "recurrence", "status"]], range_name="A1:E1")
            print("[INIT] Reminders tab created")
        print("[INIT] Sheets OK")
    except Exception as e:
        print(f"[INIT] {e}")


_init_sheets()


# ━━━ History (Sheet1) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_history(sender: str) -> list:
    with _history_lock:
        try:
            records = _get_worksheet(0).get_all_records()
            for row in records:
                if row.get("sender") == sender:
                    data = json.loads(row.get("messages", "[]"))
                    print(f"[HISTORY] Loaded {len(data)} messages", flush=True)
                    return data
            print(f"[HISTORY] No history found", flush=True)
            return []
        except Exception as e:
            print(f"[HISTORY] load error: {traceback.format_exc()}", flush=True)
            return []


def save_history(sender: str, messages: list):
    with _history_lock:
        try:
            sheet = _get_worksheet(0)
            records = sheet.get_all_records()
            for i, row in enumerate(records):
                if row.get("sender") == sender:
                    sheet.update_acell(f"B{i+2}", json.dumps(messages))
                    print(f"[HISTORY] Saved {len(messages)} messages", flush=True)
                    return
            sheet.append_row([sender, json.dumps(messages)])
            print(f"[HISTORY] Created new row with {len(messages)} messages", flush=True)
        except Exception as e:
            print(f"[HISTORY] save error: {traceback.format_exc()}", flush=True)


def clear_history(sender: str):
    with _history_lock:
        try:
            sheet = _get_worksheet(0)
            records = sheet.get_all_records()
            for i, row in enumerate(records):
                if row.get("sender") == sender:
                    sheet.update_acell(f"B{i+2}", "[]")
                    return
        except Exception as e:
            print(f"[HISTORY] clear error: {e}", flush=True)


# ━━━ Profile (Profile tab) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_profile() -> dict:
    with _profile_lock:
        try:
            records = _get_worksheet_by_name("Profile").get_all_records()
            result = {r["key"]: r["value"] for r in records if r.get("key")}
            print(f"[PROFILE] Loaded {len(result)} facts: {list(result.keys())}", flush=True)
            return result
        except Exception as e:
            print(f"[PROFILE] load error: {traceback.format_exc()}", flush=True)
            return {}


def save_fact(key: str, value: str):
    with _profile_lock:
        try:
            sheet = _get_worksheet_by_name("Profile")
            records = sheet.get_all_records()
            ts = datetime.now(AST).strftime("%Y-%m-%d %H:%M")
            for i, row in enumerate(records):
                if row.get("key", "").strip().lower() == key.strip().lower():
                    sheet.update(values=[[key, value, ts]], range_name=f"A{i+2}:C{i+2}")
                    print(f"[PROFILE] Updated: {key}", flush=True)
                    return
            sheet.append_row([key, value, ts])
            print(f"[PROFILE] Saved new: {key}", flush=True)
        except Exception as e:
            print(f"[PROFILE] save error: {traceback.format_exc()}", flush=True)


def delete_fact(key: str) -> bool:
    with _profile_lock:
        try:
            sheet = _get_worksheet_by_name("Profile")
            records = sheet.get_all_records()
            for i, row in enumerate(records):
                if row.get("key", "").strip().lower() == key.strip().lower():
                    sheet.delete_rows(i + 2)
                    return True
            return False
        except Exception as e:
            print(f"[PROFILE] delete error: {e}", flush=True)
            return False


# ━━━ Reminders (Reminders tab) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def save_reminder(message: str, due: datetime, recurrence: str = "none"):
    with _reminder_lock:
        try:
            sheet = _get_worksheet_by_name("Reminders")
            rid = str(uuid.uuid4())[:8]
            due_str = due.strftime("%Y-%m-%d %H:%M")
            sheet.append_row([rid, message, due_str, recurrence, "pending"])
            print(f"[REMINDER] Saved: {message} due {due_str}", flush=True)
            return rid
        except Exception as e:
            print(f"[REMINDER] save error: {e}", flush=True)
            return None


def load_due_reminders() -> list:
    with _reminder_lock:
        try:
            sheet = _get_worksheet_by_name("Reminders")
            records = sheet.get_all_records()
            now = datetime.now(AST)
            due = []
            for i, row in enumerate(records):
                if row.get("status") != "pending":
                    continue
                try:
                    due_time = datetime.strptime(row["due"], "%Y-%m-%d %H:%M").replace(tzinfo=AST)
                    if now >= due_time:
                        due.append({**row, "_row": i + 2})
                except Exception:
                    continue
            return due
        except Exception as e:
            print(f"[REMINDER] load error: {e}", flush=True)
            return []


def mark_reminder_done(row_num: int, recurrence: str, due_str: str):
    with _reminder_lock:
        try:
            sheet = _get_worksheet_by_name("Reminders")
            if recurrence == "none":
                sheet.update_acell(f"E{row_num}", "done")
            else:
                due = datetime.strptime(due_str, "%Y-%m-%d %H:%M").replace(tzinfo=AST)
                if recurrence == "daily":
                    next_due = due + timedelta(days=1)
                elif recurrence == "weekly":
                    next_due = due + timedelta(weeks=1)
                else:
                    sheet.update_acell(f"E{row_num}", "done")
                    return
                sheet.update_acell(f"C{row_num}", next_due.strftime("%Y-%m-%d %H:%M"))
        except Exception as e:
            print(f"[REMINDER] mark done error: {e}", flush=True)


def list_reminders() -> list:
    with _reminder_lock:
        try:
            sheet = _get_worksheet_by_name("Reminders")
            records = sheet.get_all_records()
            return [r for r in records if r.get("status") == "pending"]
        except Exception as e:
            print(f"[REMINDER] list error: {e}", flush=True)
            return []


# ━━━ Reminder Scheduler ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def reminder_scheduler():
    print("[SCHEDULER] Started", flush=True)
    while True:
        try:
            due = load_due_reminders()
            for reminder in due:
                msg = f"⏰ *Reminder:* {reminder['message']}"
                send_whatsapp(OWNER_NUMBER, msg)
                mark_reminder_done(reminder["_row"], reminder["recurrence"], reminder["due"])
                print(f"[SCHEDULER] Fired: {reminder['message']}", flush=True)
        except Exception as e:
            print(f"[SCHEDULER] Error: {e}", flush=True)
        time.sleep(60)


_scheduler_thread = threading.Thread(target=reminder_scheduler, daemon=True)
_scheduler_thread.start()


# ━━━ Market Data ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_yf_data(msg: str):
    msg_lower = msg.lower()
    matched = {}
    for keyword, ticker in YF_TICKERS.items():
        if keyword in msg_lower and ticker not in matched.values():
            matched[keyword] = ticker
    if not matched:
        return None
    results = []
    for keyword, ticker in matched.items():
        try:
            info  = yf.Ticker(ticker).fast_info
            price = info.last_price
            prev  = info.previous_close
            if price and prev:
                chg   = price - prev
                pct   = (chg / prev) * 100
                arrow = "▲" if chg >= 0 else "▼"
                results.append(f"{keyword.upper()}: {price:,.2f} {arrow} {abs(pct):.2f}%")
            elif price:
                results.append(f"{keyword.upper()}: {price:,.2f}")
        except Exception as e:
            print(f"[YFINANCE] {ticker}: {e}", flush=True)
    if not results:
        return None
    ts = datetime.now(AST).strftime("%H:%M AST")
    return "\n".join(results) + f"\n_{ts}_"


def is_yf_query(msg: str) -> bool:
    return any(t in msg.lower() for t in YF_TRIGGERS)


def is_saudi_query(msg: str) -> bool:
    return any(t in msg.lower() for t in SAUDI_TRIGGERS)


# ━━━ Messaging ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def split_message(text: str, limit: int = MAX_MSG_LEN) -> list:
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        for sep in ["\n\n", ". ", "\n", " "]:
            pos = text.rfind(sep, 0, limit)
            if pos > 0:
                if sep == ". ":
                    pos += 1
                break
        else:
            pos = limit
        chunks.append(text[:pos].strip())
        text = text[pos:].strip()
    return [c for c in chunks if c]


def send_whatsapp(to: str, text: str):
    for i, chunk in enumerate(split_message(text)):
        try:
            twilio_client.messages.create(body=chunk, from_=TWILIO_NUMBER, to=to)
            if i > 0:
                time.sleep(0.4)
        except Exception as e:
            print(f"[TWILIO] chunk {i}: {e}", flush=True)


def send_error(to: str, msg: str = "Something went wrong. Please try again."):
    try:
        twilio_client.messages.create(body=f"⚠️ {msg}", from_=TWILIO_NUMBER, to=to)
    except Exception:
        pass


# ━━━ Security ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def validate_twilio(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if app.debug:
            return f(*args, **kwargs)
        sig = request.headers.get("X-Twilio-Signature", "")
        url = request.url
        if request.headers.get("X-Forwarded-Proto") == "https":
            url = url.replace("http://", "https://", 1)
        if not twilio_validator.validate(url, request.form, sig):
            abort(403)
        return f(*args, **kwargs)
    return wrapper


# ━━━ System Prompt ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SYSTEM_PROMPT = """You are Safa7 — a sharp, discreet personal assistant for a senior tourism, hospitality, investment and finance professional based in Saudi Arabia.

YOUR PRIMARY JOB: Help manage tasks, team, priorities, meetings, follow-ups, and reminders. You are a trusted chief of staff.

MEMORY RULES:
- Your profile facts below are your long-term memory. Trust them completely.
- Conversation history is your short-term memory. Use it fully.
- If something is in your profile facts OR conversation history — you know it. Never say "I don't have that information."
- When told about tasks, meetings, team updates — confirm clearly that you have it.

REMINDER RULES:
- When the user asks you to remind them of something, respond with ONLY a brief confirmation line followed by the JSON block. Keep it short.
- Format exactly like this:
  Got it. Reminding you to [task] on [date] at [time].
  REMINDER_JSON: {{"message": "task description", "due": "YYYY-MM-DD HH:MM", "recurrence": "none"}}
- Recurrence options: "none", "daily", "weekly"
- For relative times: "tomorrow" = next day, "next week" = 7 days from now, "every morning at 8am" = daily at 08:00
- Keep the confirmation text BEFORE the JSON, never after. One line only.

COMMUNICATION RULES:
- Match user language (Arabic/English/mixed).
- Lead with the answer. Be concise.
- For tasks/team: organized, clear, proactive.
- For market data: number first, source second, one line.
- Never hedge. Never say "I recommend checking elsewhere."
- No preamble. No "Based on..." or "Let me search..."
- If a request is ambiguous, ask ONE clarifying question before acting.

MARKET DATA — CRITICAL:
- TASI and all Saudi market data: ONLY use mubasher.info or saudiexchange.sa. Never use investing.com, Yahoo Finance, or yfinance for Saudi data.
- Oil/FX: injected directly — present cleanly.
- Format: "TASI closed at 11,007.19 (+2.14%) — Mubasher."

YOUR LONG-TERM MEMORY:
{profile_facts}

Current time: {current_time}"""


def build_system_prompt(profile: dict) -> str:
    pf  = "\n".join(f"- {k}: {v}" for k, v in profile.items()) if profile else "No facts saved yet."
    now = datetime.now(AST).strftime("%A, %B %d, %Y at %H:%M AST")
    return SYSTEM_PROMPT.format(profile_facts=pf, current_time=now)


# ━━━ Reminder Extraction ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def extract_and_save_reminder(reply: str) -> str:
    if "REMINDER_JSON:" not in reply:
        return reply
    try:
        parts      = reply.split("REMINDER_JSON:")
        clean_text = parts[0].strip()
        json_str   = parts[1].strip()
        start      = json_str.index("{")
        end        = json_str.rindex("}") + 1
        data       = json.loads(json_str[start:end])

        due_dt     = datetime.strptime(data["due"], "%Y-%m-%d %H:%M").replace(tzinfo=AST)
        recurrence = data.get("recurrence", "none")
        save_reminder(data["message"], due_dt, recurrence)

        due_fmt = due_dt.strftime("%A, %B %d at %H:%M AST")
        rec_txt = " (repeats daily)" if recurrence == "daily" else " (repeats weekly)" if recurrence == "weekly" else ""
        return f"{clean_text}\n⏰ Reminder set: *{data['message']}* — {due_fmt}{rec_txt}".strip()
    except Exception as e:
        print(f"[REMINDER] extract error: {e}", flush=True)
        # Never crash — strip broken JSON and return clean text
        return reply.split("REMINDER_JSON:")[0].strip() or reply


# ━━━ Commands ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def handle_command(msg: str, sender: str):
    raw = msg.strip()
    low = raw.lower()

    if low == "!help":
        return (
            "📖 *Safa7 Commands*\n\n"
            "• `!remember key: value` — Save a permanent fact\n"
            "• `!facts` — View all saved facts\n"
            "• `!forget [key]` — Remove a saved fact\n"
            "• `!reminders` — View pending reminders\n"
            "• `!clear` — Reset conversation history\n"
            "• `!status` — System status\n"
            "• `!help` — This message\n\n"
            "💡 Set reminders naturally:\n"
            "\"Remind me tomorrow at 9am to call Albahiti\"\n"
            "\"Remind me every Monday at 8am to review the team\""
        )

    if low == "!clear":
        clear_history(sender)
        return "🗑️ Conversation history cleared. Profile facts and reminders kept."

    if low == "!status":
        profile   = load_profile()
        history   = load_history(sender)
        reminders = list_reminders()
        now = datetime.now(AST).strftime("%Y-%m-%d %H:%M AST")
        return (
            f"🟢 *Safa7 Online*\n"
            f"⏰ {now}\n"
            f"🤖 {MODEL}\n"
            f"💬 {len(history)} messages in history\n"
            f"📋 {len(profile)} saved facts\n"
            f"🔔 {len(reminders)} pending reminders\n"
            f"📏 History limit: {MAX_HISTORY} messages"
        )

    if low == "!reminders":
        reminders = list_reminders()
        if not reminders:
            return "🔔 No pending reminders."
        lines = [f"• [{r['id']}] {r['message']} — {r['due']} ({r['recurrence']})" for r in reminders]
        return "🔔 *Pending Reminders*\n" + "\n".join(lines)

    if low == "!facts":
        profile = load_profile()
        if not profile:
            return "📋 No saved facts yet. Use `!remember key: value` to save one."
        lines = [f"• *{k}*: {v}" for k, v in profile.items()]
        return "📋 *Saved Facts*\n" + "\n".join(lines)

    if low.startswith("!remember "):
        fact = raw[10:].strip()
        if not fact:
            return "Usage: `!remember key: value`"
        if ":" in fact and fact.index(":") < 60:
            key, value = fact.split(":", 1)
            key, value = key.strip(), value.strip()
            if not value:
                value = fact
        else:
            key = fact[:60].strip()
            value = fact
        save_fact(key, value)
        return f"✅ Saved to permanent memory: *{key}*"

    if low.startswith("!forget "):
        key = raw[8:].strip()
        if not key:
            return "Usage: `!forget [key]`"
        if delete_fact(key):
            return f"🗑️ Removed: *{key}*"
        return f"❌ No fact found matching: *{key}*. Use `!facts` to see what's saved."

    return None


# ━━━ Claude ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def clean_reply(reply: str) -> str:
    lines = reply.split("\n")
    noise = [
        "based on the search", "let me search", "i can see that",
        "i'm seeing", "i will search", "i should search",
        "search results show", "according to my search"
    ]
    clean = [l for l in lines if not any(p in l.lower() for p in noise)]
    result = "\n".join(clean).strip()
    return result if result else reply.strip()


def call_claude(history: list, profile: dict) -> str:
    try:
        system = build_system_prompt(profile)
        tools = [{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": 2,
            "user_location": {
                "type": "approximate",
                "country": "SA",
                "timezone": "Asia/Riyadh"
            }
        }]

        response = claude.messages.create(
            model=MODEL, max_tokens=MAX_TOKENS,
            system=system, tools=tools, messages=history
        )

        if response.stop_reason == "pause_turn":
            response = claude.messages.create(
                model=MODEL, max_tokens=MAX_TOKENS,
                system=system, tools=tools,
                messages=history + [
                    {"role": "assistant", "content": response.content},
                    {"role": "user", "content": "Continue."}
                ]
            )

        parts = [b.text for b in response.content if getattr(b, "type", "") == "text"]
        reply = "\n".join(parts).strip() or "I couldn't generate a response. Try again."
        print(f"[CLAUDE] Reply: {reply[:80]}", flush=True)
        return clean_reply(reply)

    except Exception as e:
        print(f"[CLAUDE ERROR] {traceback.format_exc()}", flush=True)
        raise


# ━━━ Processing Pipeline ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_message(incoming_msg: str, sender: str):
    lock = _get_sender_lock(sender)
    with lock:
        try:
            print(f"[MSG] From {sender}: {incoming_msg[:60]}", flush=True)

            # 1. Commands
            if incoming_msg.startswith("!"):
                result = handle_command(incoming_msg, sender)
                if result:
                    send_whatsapp(sender, result)
                    return

            # 2. Load history + profile upfront
            history = load_history(sender)
            profile = load_profile()
            print(f"[PROCESS] History: {len(history)} msgs, Profile: {len(profile)} facts", flush=True)

            history.append({"role": "user", "content": incoming_msg})

            # 3. yfinance shortcut for oil/FX only
            if is_yf_query(incoming_msg) and not is_saudi_query(incoming_msg):
                market_data = get_yf_data(incoming_msg)
                if market_data:
                    history.append({"role": "assistant", "content": market_data})
                    if len(history) > MAX_HISTORY:
                        history = history[-MAX_HISTORY:]
                    save_history(sender, history)
                    send_whatsapp(sender, market_data)
                    return

            # 4. Claude with full history + profile
            reply = call_claude(history, profile)

            # 5. Extract and save any reminders
            reply = extract_and_save_reminder(reply)

            history.append({"role": "assistant", "content": reply})
            if len(history) > MAX_HISTORY:
                history = history[-MAX_HISTORY:]
            save_history(sender, history)
            send_whatsapp(sender, reply)

        except anthropic.RateLimitError:
            send_error(sender, "Rate limited. Wait a minute and try again.")
        except anthropic.APIStatusError as e:
            if e.status_code == 529:
                send_error(sender, "AI service overloaded. Try again shortly.")
            else:
                print(f"[CLAUDE] API error {e.status_code}: {e}", flush=True)
                send_error(sender)
        except Exception as e:
            print(f"[ERROR] {traceback.format_exc()}", flush=True)
            send_error(sender)


# ━━━ Routes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.route("/webhook", methods=["POST"])
@validate_twilio
def webhook():
    body   = request.values.get("Body", "").strip()
    sender = request.values.get("From", "")
    if not body or not sender:
        return str(MessagingResponse()), 200
    _executor.submit(process_message, body, sender)
    return str(MessagingResponse()), 200


@app.route("/health", methods=["GET"])
def health():
    reminders = list_reminders()
    return {"status": "ok", "model": MODEL, "pending_reminders": len(reminders)}, 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
