"""
Safa7 — WhatsApp AI Assistant
Flask + Twilio + Anthropic Claude + Google Sheets
"""

import os
import json
import time
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

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
MAX_HISTORY = 30
MAX_MSG_LEN = 1600
AST = timezone(timedelta(hours=3))


# ━━━ Clients ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

claude = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "whatsapp:+14155238886")
twilio_client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
twilio_validator = RequestValidator(TWILIO_TOKEN)

_GS_CREDS = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
_GS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")


# ━━━ Thread Safety ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_sheet_lock = threading.Lock()
_sender_locks = {}
_sender_locks_meta = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=3)
_gs_client = None


def _get_sender_lock(sender):
    with _sender_locks_meta:
        if sender not in _sender_locks:
            _sender_locks[sender] = threading.Lock()
        return _sender_locks[sender]


# ━━━ Google Sheets Layer ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gs():
    global _gs_client
    if _gs_client is None:
        creds = Credentials.from_service_account_info(_GS_CREDS, scopes=_GS_SCOPES)
        _gs_client = gspread.authorize(creds)
    return _gs_client


def _sheet(tab=0):
    return _gs().open_by_key(SHEET_ID).get_worksheet(tab)


def _init_sheets():
    if not _GS_CREDS or not SHEET_ID:
        return
    try:
        with _sheet_lock:
            spreadsheet = _gs().open_by_key(SHEET_ID)
            titles = [ws.title for ws in spreadsheet.worksheets()]
            if "Profile" not in titles:
                ws = spreadsheet.add_worksheet(title="Profile", rows=100, cols=3)
                ws.update(values=[["key", "value", "updated"]], range_name="A1:C1")
                print("[INIT] Profile tab created")
    except Exception as e:
        print(f"[INIT] Sheet setup error: {e}")


_init_sheets()


# ━━━ History (Tab 0) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_history(sender):
    with _sheet_lock:
        try:
            records = _sheet(0).get_all_records()
            for row in records:
                if row.get("sender") == sender:
                    return json.loads(row.get("messages", "[]"))
            return []
        except Exception as e:
            print(f"[SHEETS] load_history: {e}")
            return []


def save_history(sender, messages):
    with _sheet_lock:
        try:
            sheet = _sheet(0)
            records = sheet.get_all_records()
            for i, row in enumerate(records):
                if row.get("sender") == sender:
                    sheet.update_acell(f"B{i+2}", json.dumps(messages))
                    return
            sheet.append_row([sender, json.dumps(messages)])
        except Exception as e:
            print(f"[SHEETS] save_history: {e}")


def clear_history(sender):
    with _sheet_lock:
        try:
            sheet = _sheet(0)
            records = sheet.get_all_records()
            for i, row in enumerate(records):
                if row.get("sender") == sender:
                    sheet.update_acell(f"B{i+2}", "[]")
                    return
        except Exception as e:
            print(f"[SHEETS] clear_history: {e}")


# ━━━ Profile Facts (Tab 1) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_profile():
    with _sheet_lock:
        try:
            records = _sheet(1).get_all_records()
            return {r["key"]: r["value"] for r in records if r.get("key")}
        except Exception as e:
            print(f"[SHEETS] load_profile: {e}")
            return {}


def save_fact(key, value):
    with _sheet_lock:
        try:
            sheet = _sheet(1)
            records = sheet.get_all_records()
            ts = datetime.now(AST).strftime("%Y-%m-%d %H:%M")
            for i, row in enumerate(records):
                if row.get("key", "").strip().lower() == key.strip().lower():
                    sheet.update(
                        values=[[key, value, ts]], range_name=f"A{i+2}:C{i+2}"
                    )
                    return
            sheet.append_row([key, value, ts])
        except Exception as e:
            print(f"[SHEETS] save_fact: {e}")


def delete_fact(key):
    with _sheet_lock:
        try:
            sheet = _sheet(1)
            records = sheet.get_all_records()
            for i, row in enumerate(records):
                if row.get("key", "").strip().lower() == key.strip().lower():
                    sheet.delete_rows(i + 2)
                    return True
            return False
        except Exception as e:
            print(f"[SHEETS] delete_fact: {e}")
            return False


# ━━━ Messaging ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def split_message(text, limit=MAX_MSG_LEN):
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        pos = text.rfind("\n\n", 0, limit)
        if pos < 1:
            pos = text.rfind(". ", 0, limit)
            if pos > 0:
                pos += 1
        if pos < 1:
            pos = text.rfind("\n", 0, limit)
        if pos < 1:
            pos = text.rfind(" ", 0, limit)
        if pos < 1:
            pos = limit
        chunks.append(text[:pos].strip())
        text = text[pos:].strip()
    return [c for c in chunks if c]


def send_whatsapp(to, text):
    chunks = split_message(text)
    for i, chunk in enumerate(chunks):
        try:
            twilio_client.messages.create(body=chunk, from_=TWILIO_NUMBER, to=to)
            if len(chunks) > 1 and i < len(chunks) - 1:
                time.sleep(0.3)
        except Exception as e:
            print(f"[TWILIO] send error chunk {i+1}/{len(chunks)}: {e}")


def send_error(to, msg="Something went wrong. Please try again in a moment."):
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

SYSTEM_PROMPT = """You are Safa7. You give ONE sentence answers to market data questions. Number first. Source second. Nothing else. No explanations. No caveats. No "however". No "I notice". Just the fact.

RULES — no exceptions:
1. Market data: state the number first, source second, one line. Done.
2. If search results show a clear number, use it. Don't debate it.
3. Never say "I cannot confirm" or "you may need to check" — pick the best number available and state it.
4. Maximum 2 sentences for any market query.
5. Match user language (Arabic/English/mixed).
6. No preamble. No hedging. No narrating your search process.

Example of correct response to "What did TASI close at?":
"TASI closed at 11,007.19 (+2.14%) on March 8 — Mubasher."

<web_search>
Use search for: prices, indices, news, rates, earnings, IPOs, regulations.
Skip search for: definitions, math, general knowledge, stable facts.
Pick ONE best source. State the number. Stop.
</web_search>

<user_context>
{profile_facts}
</user_context>

<current_time>{current_time}</current_time>"""


def _build_system_prompt():
    facts = load_profile()
    pf = "\n".join(f"- {k}: {v}" for k, v in facts.items()) if facts \
        else "No profile facts saved yet."
    now = datetime.now(AST).strftime("%A, %B %d, %Y at %H:%M AST")
    return SYSTEM_PROMPT.format(profile_facts=pf, current_time=now)


# ━━━ Commands ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def handle_command(msg, sender):
    raw = msg.strip()
    low = raw.lower()

    if low == "!help":
        return (
            "📖 *Safa7 Commands*\n\n"
            "• `!remember key: value` — Save a persistent fact\n"
            "• `!facts` — View all saved facts\n"
            "• `!forget [key]` — Remove a saved fact\n"
            "• `!clear` — Reset conversation history\n"
            "• `!status` — System status\n"
            "• `!help` — This message"
        )

    if low == "!clear":
        clear_history(sender)
        return "🗑️ History cleared."

    if low == "!status":
        facts = load_profile()
        hist = load_history(sender)
        now = datetime.now(AST).strftime("%Y-%m-%d %H:%M AST")
        return (
            f"🟢 *Safa7 Online*\n"
            f"⏰ {now}\n"
            f"🤖 {MODEL}\n"
            f"💬 {len(hist)} messages in history\n"
            f"📋 {len(facts)} saved facts\n"
            f"📏 History limit: {30} messages"
        )

    if low == "!facts":
        facts = load_profile()
        if not facts:
            return "📋 No saved facts yet. Use `!remember key: value` to save one."
        lines = [f"• *{k}*: {v}" for k, v in facts.items()]
        return "📋 *Saved Facts*\n" + "\n".join(lines)

    if low.startswith("!remember "):
        fact = raw[10:].strip()
        if not fact:
            return "Usage: `!remember key: value` or `!remember [fact]`"
        if ":" in fact and fact.index(":") < 60:
            key, value = fact.split(":", 1)
            key, value = key.strip(), value.strip()
            if not value:
                value = fact
        else:
            key = fact[:60].strip()
            value = fact
        save_fact(key, value)
        return f"✅ Saved: *{key}*"

    if low.startswith("!forget "):
        key = raw[8:].strip()
        if not key:
            return "Usage: `!forget [key]` — use `!facts` to see your keys."
        if delete_fact(key):
            return f"🗑️ Removed: *{key}*"
        return f"❌ No fact found matching: *{key}*"

    return None


# ━━━ Claude ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def call_claude(history):
    system = _build_system_prompt()
    tools = [{
        "type": "web_search_20250305",
        "name": "web_search",
        "max_uses": 3,
        "user_location": {
            "type": "approximate",
            "country": "SA",
            "timezone": "Asia/Riyadh"
        }
    }]

    response = claude.messages.create(
        model=MODEL, max_tokens=1024, system=system,
        tools=tools, messages=history
    )

    if response.stop_reason == "pause_turn":
        response = claude.messages.create(
            model=MODEL, max_tokens=1024, system=system, tools=tools,
            messages=history + [
                {"role": "assistant", "content": response.content},
                {"role": "user", "content": "Continue."}
            ]
        )

    parts = [b.text for b in response.content if getattr(b, "type", "") == "text"]
    return "\n".join(parts).strip() or "I couldn't generate a response. Try again."


# ━━━ Processing Pipeline ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_message(incoming_msg, sender):
    lock = _get_sender_lock(sender)
    with lock:
        try:
            if incoming_msg.startswith("!"):
                result = handle_command(incoming_msg, sender)
                if result:
                    send_whatsapp(sender, result)
                    return

            history = load_history(sender)
            history.append({"role": "user", "content": incoming_msg})

            reply = call_claude(history)

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
                print(f"[CLAUDE] {e.status_code}: {e}")
                send_error(sender)
        except Exception as e:
            print(f"[ERROR] {e}")
            send_error(sender)


# ━━━ Routes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.route("/webhook", methods=["POST"])
@validate_twilio
def webhook():
    body = request.values.get("Body", "").strip()
    sender = request.values.get("From", "")

    if not body or not sender:
        return str(MessagingResponse()), 200

    _executor.submit(process_message, body, sender)
    return str(MessagingResponse()), 200


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "model": MODEL}, 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
