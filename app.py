"""
Safa7 — WhatsApp AI Assistant
Flask + Twilio + Anthropic Claude + Google Sheets + yfinance
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


# ━━━ Config ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app = Flask(__name__)

MODEL       = "claude-sonnet-4-20250514"
MAX_HISTORY = 50
MAX_MSG_LEN = 1600
MAX_TOKENS  = 3000
AST         = timezone(timedelta(hours=3))

YF_TICKERS = {
    "brent":   "BZ=F",
    "wti":     "CL=F",
    "eur/usd": "EURUSD=X",
    "gbp/usd": "GBPUSD=X",
    "s&p 500": "^GSPC",
    "nasdaq":  "^IXIC",
}
YF_TRIGGERS = list(YF_TICKERS.keys())

SAUDI_TRIGGERS = [
    "tasi", "tadawul", "aramco", "sabic", "stc", "alrajhi", "al rajhi",
    "riyad bank", "alinma", "saudi stock", "saudi market", "saudi exchange",
    "usd/sar", "usd sar", "dollar sar", "ريال", "تاسي", "تداول", "أرامكو",
]


# ━━━ Clients ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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


# ━━━ Thread Safety ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_sender_locks      = {}
_sender_locks_meta = threading.Lock()
_executor          = ThreadPoolExecutor(max_workers=4)
_gs_lock           = threading.Lock()


def _get_sender_lock(sender: str) -> threading.Lock:
    with _sender_locks_meta:
        if sender not in _sender_locks:
            _sender_locks[sender] = threading.Lock()
        return _sender_locks[sender]


# ━━━ Google Sheets ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gs_client():
    creds = Credentials.from_service_account_info(_GS_CREDS, scopes=_GS_SCOPES)
    return gspread.authorize(creds)


def _open_sheet():
    """Open the spreadsheet and return it (single auth call)."""
    return _gs_client().open_by_key(SHEET_ID)


def _init_sheets():
    if not _GS_CREDS or not SHEET_ID:
        return
    try:
        ss     = _open_sheet()
        titles = [ws.title for ws in ss.worksheets()]
        if "Profile" not in titles:
            ws = ss.add_worksheet(title="Profile", rows=200, cols=3)
            ws.update(values=[["key", "value", "updated"]], range_name="A1:C1")
            print("[INIT] Profile tab created", flush=True)
        if "Reminders" not in titles:
            ws = ss.add_worksheet(title="Reminders", rows=200, cols=5)
            ws.update(values=[["id", "message", "due", "recurrence", "status"]], range_name="A1:E1")
            print("[INIT] Reminders tab created", flush=True)
        print("[INIT] Sheets OK", flush=True)
    except Exception as e:
        print(f"[INIT] {e}", flush=True)


_init_sheets()


# ━━━ Batch Load (one auth, one open, three reads) ━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_all(sender: str):
    """
    Returns (history: list, profile: dict, reminders: list)
    All three tabs in a single spreadsheet open — minimises latency.
    """
    try:
        with _gs_lock:
            ss       = _open_sheet()
            ws_hist  = ss.get_worksheet(0)           # Sheet1
            ws_prof  = ss.worksheet("Profile")
            ws_rem   = ss.worksheet("Reminders")

            hist_rows = ws_hist.get_all_records()
            prof_rows = ws_prof.get_all_records()
            rem_rows  = ws_rem.get_all_records()

        # history
        history = []
        for row in hist_rows:
            if row.get("sender") == sender:
                history = json.loads(row.get("messages", "[]"))
                break

        # profile
        profile = {r["key"]: r["value"] for r in prof_rows if r.get("key")}

        # reminders (pending only)
        now = datetime.now(AST)
        pending = []
        for i, r in enumerate(rem_rows):
            if r.get("status") == "pending":
                pending.append({**r, "_row": i + 2})

        print(f"[LOAD] history={len(history)} profile={len(profile)} reminders={len(pending)}", flush=True)
        return history, profile, pending

    except Exception as e:
        print(f"[LOAD] error: {traceback.format_exc()}", flush=True)
        return [], {}, []


# ━━━ History ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def save_history(sender: str, messages: list):
    try:
        with _gs_lock:
            sheet   = _open_sheet().get_worksheet(0)
            records = sheet.get_all_records()
            for i, row in enumerate(records):
                if row.get("sender") == sender:
                    sheet.update_acell(f"B{i+2}", json.dumps(messages))
                    print(f"[HISTORY] Saved {len(messages)} msgs", flush=True)
                    return
            sheet.append_row([sender, json.dumps(messages)])
            print(f"[HISTORY] New row {len(messages)} msgs", flush=True)
    except Exception:
        print(f"[HISTORY] save error: {traceback.format_exc()}", flush=True)


def clear_history(sender: str):
    try:
        with _gs_lock:
            sheet   = _open_sheet().get_worksheet(0)
            records = sheet.get_all_records()
            for i, row in enumerate(records):
                if row.get("sender") == sender:
                    sheet.update_acell(f"B{i+2}", "[]")
                    return
    except Exception as e:
        print(f"[HISTORY] clear error: {e}", flush=True)


# ━━━ Profile ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def save_fact(key: str, value: str):
    try:
        with _gs_lock:
            sheet   = _open_sheet().worksheet("Profile")
            records = sheet.get_all_records()
            ts      = datetime.now(AST).strftime("%Y-%m-%d %H:%M")
            for i, row in enumerate(records):
                if row.get("key", "").strip().lower() == key.strip().lower():
                    sheet.update(values=[[key, value, ts]], range_name=f"A{i+2}:C{i+2}")
                    print(f"[PROFILE] Updated: {key}", flush=True)
                    return
            sheet.append_row([key, value, ts])
            print(f"[PROFILE] New: {key}", flush=True)
    except Exception:
        print(f"[PROFILE] save error: {traceback.format_exc()}", flush=True)


def delete_fact(key: str) -> bool:
    try:
        with _gs_lock:
            sheet   = _open_sheet().worksheet("Profile")
            records = sheet.get_all_records()
            for i, row in enumerate(records):
                if row.get("key", "").strip().lower() == key.strip().lower():
                    sheet.delete_rows(i + 2)
                    return True
        return False
    except Exception as e:
        print(f"[PROFILE] delete error: {e}", flush=True)
        return False


# ━━━ Reminders ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def save_reminder(message: str, due: datetime, recurrence: str = "none") -> str:
    try:
        with _gs_lock:
            sheet   = _open_sheet().worksheet("Reminders")
            rid     = str(uuid.uuid4())[:8]
            due_str = due.strftime("%Y-%m-%d %H:%M")
            sheet.append_row([rid, message, due_str, recurrence, "pending"])
            print(f"[REMINDER] Saved: {message} @ {due_str}", flush=True)
            return rid
    except Exception as e:
        print(f"[REMINDER] save error: {e}", flush=True)
        return ""


def mark_reminder_done(row_num: int, recurrence: str, due_str: str):
    try:
        with _gs_lock:
            sheet = _open_sheet().worksheet("Reminders")
            if recurrence == "none":
                sheet.update_acell(f"E{row_num}", "done")
            else:
                due = datetime.strptime(due_str, "%Y-%m-%d %H:%M").replace(tzinfo=AST)
                delta = timedelta(days=1) if recurrence == "daily" else timedelta(weeks=1)
                sheet.update_acell(f"C{row_num}", (due + delta).strftime("%Y-%m-%d %H:%M"))
    except Exception as e:
        print(f"[REMINDER] mark done error: {e}", flush=True)


def list_reminders_raw() -> list:
    try:
        with _gs_lock:
            records = _open_sheet().worksheet("Reminders").get_all_records()
        return [r for r in records if r.get("status") == "pending"]
    except Exception as e:
        print(f"[REMINDER] list error: {e}", flush=True)
        return []


# ━━━ Reminder Scheduler ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def reminder_scheduler():
    print("[SCHEDULER] Started", flush=True)
    while True:
        try:
            _, _, pending = load_all(OWNER_NUMBER)
            now = datetime.now(AST)
            for r in pending:
                try:
                    due_time = datetime.strptime(r["due"], "%Y-%m-%d %H:%M").replace(tzinfo=AST)
                    if now >= due_time:
                        send_whatsapp(OWNER_NUMBER, f"⏰ *Reminder:* {r['message']}")
                        mark_reminder_done(r["_row"], r["recurrence"], r["due"])
                        print(f"[SCHEDULER] Fired: {r['message']}", flush=True)
                except Exception:
                    pass
        except Exception as e:
            print(f"[SCHEDULER] Error: {e}", flush=True)
        time.sleep(60)


# ━━━ Market Data ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_yf_data(msg: str):
    msg_lower = msg.lower()
    matched   = {kw: tk for kw, tk in YF_TICKERS.items() if kw in msg_lower}
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
            print(f"[YF] {ticker}: {e}", flush=True)
    if not results:
        return None
    return "\n".join(results) + f"\n_{datetime.now(AST).strftime('%H:%M AST')}_"


def is_yf_query(msg: str) -> bool:
    return any(t in msg.lower() for t in YF_TRIGGERS)


def is_saudi_query(msg: str) -> bool:
    return any(t in msg.lower() for t in SAUDI_TRIGGERS)


# ━━━ Messaging ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
            print(f"[SENT] chunk {i+1}: {chunk[:60]}", flush=True)
            if i > 0:
                time.sleep(0.4)
        except Exception as e:
            print(f"[TWILIO] chunk {i}: {e}", flush=True)


def send_error(to: str, msg: str = "Something went wrong. Please try again."):
    try:
        twilio_client.messages.create(body=f"⚠️ {msg}", from_=TWILIO_NUMBER, to=to)
    except Exception:
        pass


# ━━━ Security ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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


# ━━━ System Prompt ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SYSTEM_PROMPT = """You are Safa7 — a sharp, discreet personal assistant for a senior tourism, hospitality, investment and finance professional in Saudi Arabia. You are his trusted chief of staff.

PRIMARY JOB: Tasks, team, priorities, meetings, follow-ups, reminders.

MEMORY: Profile facts = long-term memory. History = short-term. If it's in either — you know it. Never say "I don't have that information."

REMINDERS — CRITICAL RULES:
When asked to set a reminder, respond with EXACTLY this format and nothing else:
Got it. Reminding you to [task] on [date] at [time].
REMINDER_JSON: {{"message": "task description", "due": "YYYY-MM-DD HH:MM", "recurrence": "none"}}

- Recurrence: "none", "daily", or "weekly"
- Current time is injected below — use it to resolve "today", "tomorrow", "next week"
- Keep confirmation to ONE line before the JSON. Nothing after the JSON.
- JSON must be complete and valid. Never truncate it.

COMMUNICATION:
- Match user language (Arabic/English/mixed). Lead with answer. Be concise.
- No preamble. No "Based on..." or "Let me search...". No hedging.
- If ambiguous, ask ONE clarifying question before acting.

MARKET DATA:
- TASI + Saudi stocks: ONLY mubasher.info or saudiexchange.sa. Never investing.com or yfinance.
- Oil/FX data is injected directly — present it cleanly. Format: "Brent: 74.20 ▲ 0.3%"

TEAM: Algazlan, Altayash, Almazyad, Alanoud, Bandar, Yasir, Abdullah

YOUR MEMORY:
{profile_facts}

Now: {current_time}"""


def build_system_prompt(profile: dict) -> str:
    pf  = "\n".join(f"- {k}: {v}" for k, v in profile.items()) if profile else "No facts saved yet."
    now = datetime.now(AST).strftime("%A, %B %d, %Y at %H:%M AST")
    return SYSTEM_PROMPT.format(profile_facts=pf, current_time=now)


# ━━━ Reminder Extraction ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
        print(f"[REMINDER] extract error: {e} | raw: {reply[reply.find('REMINDER_JSON:'):reply.find('REMINDER_JSON:')+120]}", flush=True)
        # Never crash — strip broken JSON, return clean text
        return reply.split("REMINDER_JSON:")[0].strip() or reply


# ━━━ Commands ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
        reminders = list_reminders_raw()
        history, profile, _ = load_all(sender)
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
        reminders = list_reminders_raw()
        if not reminders:
            return "🔔 No pending reminders."
        lines = [f"• [{r['id']}] {r['message']} — {r['due']} ({r['recurrence']})" for r in reminders]
        return "🔔 *Pending Reminders*\n" + "\n".join(lines)

    if low == "!facts":
        _, profile, _ = load_all(sender)
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
            key   = fact[:60].strip()
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


# ━━━ Claude ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def clean_reply(reply: str) -> str:
    noise = [
        "based on the search", "let me search", "i can see that",
        "i'm seeing", "i will search", "i should search",
        "search results show", "according to my search"
    ]
    lines  = reply.split("\n")
    clean  = [l for l in lines if not any(p in l.lower() for p in noise)]
    result = "\n".join(clean).strip()
    return result if result else reply.strip()


def call_claude(history: list, profile: dict) -> str:
    system = build_system_prompt(profile)
    tools  = [{
        "type": "web_search_20250305",
        "name": "web_search",
        "max_uses": 2,
        "user_location": {"type": "approximate", "country": "SA", "timezone": "Asia/Riyadh"}
    }]

    response = claude.messages.create(
        model=MODEL, max_tokens=MAX_TOKENS,
        system=system, tools=tools, messages=history
    )

    # Handle pause_turn (web search mid-stream)
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
    print(f"[CLAUDE] Reply ({len(reply)} chars): {reply[:100]}", flush=True)
    return clean_reply(reply)


# ━━━ Processing Pipeline ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_message(incoming_msg: str, sender: str):
    lock = _get_sender_lock(sender)
    with lock:
        try:
            print(f"[MSG] From {sender}: {incoming_msg[:80]}", flush=True)

            # 1. Commands — fast path, no Sheets needed for most
            if incoming_msg.startswith("!"):
                result = handle_command(incoming_msg, sender)
                if result:
                    send_whatsapp(sender, result)
                    return

            # 2. Single batch load — history + profile + reminders in one go
            history, profile, _ = load_all(sender)
            print(f"[PROCESS] history={len(history)} profile={len(profile)}", flush=True)

            history.append({"role": "user", "content": incoming_msg})

            # 3. yfinance shortcut for oil/FX (not Saudi stocks)
            if is_yf_query(incoming_msg) and not is_saudi_query(incoming_msg):
                market_data = get_yf_data(incoming_msg)
                if market_data:
                    history.append({"role": "assistant", "content": market_data})
                    if len(history) > MAX_HISTORY:
                        history = history[-MAX_HISTORY:]
                    save_history(sender, history)
                    send_whatsapp(sender, market_data)
                    return

            # 4. Claude
            reply = call_claude(history, profile)

            # 5. Extract + save reminder BEFORE sending
            reply = extract_and_save_reminder(reply)

            # 6. Update history
            history.append({"role": "assistant", "content": reply})
            if len(history) > MAX_HISTORY:
                history = history[-MAX_HISTORY:]
            save_history(sender, history)

            # 7. Send
            send_whatsapp(sender, reply)

        except anthropic.RateLimitError:
            send_error(sender, "Rate limited. Wait a minute and try again.")
        except anthropic.APIStatusError as e:
            if e.status_code == 529:
                send_error(sender, "AI service overloaded. Try again shortly.")
            else:
                print(f"[CLAUDE] API error {e.status_code}: {e}", flush=True)
                send_error(sender)
        except Exception:
            print(f"[ERROR] {traceback.format_exc()}", flush=True)
            send_error(sender)


# ━━━ Routes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.route("/webhook", methods=["POST"])
@validate_twilio
def webhook():
    body   = request.values.get("Body", "").strip()
    sender = request.values.get("From", "")
    if not body or not sender:
        return str(MessagingResponse()), 200
    _executor.submit(process_message, body, sender)
    return str(MessagingResponse()), 200


@app.route("/", methods=["GET"])
@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "model": MODEL}, 200


def start_scheduler():
    """Called by gunicorn post_fork hook — runs inside the worker, after port is bound."""
    threading.Thread(target=reminder_scheduler, daemon=True).start()


if __name__ == "__main__":
    start_scheduler()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
