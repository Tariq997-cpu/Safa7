import os
import json
import threading
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client as TwilioClient
import anthropic

app = Flask(__name__)
claude = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
TWILIO_NUMBER = "whatsapp:+14155238886"

def get_sheet():
    creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID).sheet1

def load_memory():
    try:
        sheet = get_sheet()
        records = sheet.get_all_records()
        memory = {}
        for row in records:
            sender = row.get("sender")
            messages = json.loads(row.get("messages", "[]"))
            if sender:
                memory[sender] = messages
        return memory
    except Exception as e:
        print(f"Memory load error: {e}")
        return {}

def save_memory(sender, messages):
    try:
        sheet = get_sheet()
        records = sheet.get_all_records()
        for i, row in enumerate(records):
            if row.get("sender") == sender:
                sheet.update(f"B{i+2}", json.dumps(messages))
                return
        sheet.append_row([sender, json.dumps(messages)])
    except Exception as e:
        print(f"Memory save error: {e}")

def needs_search(msg):
    keywords = ["price", "news", "today", "current", "latest", "now", "weather", "stock", "rate", "2025", "2026", "tasi", "market", "index"]
    return any(k in msg.lower() for k in keywords)

def process_and_reply(incoming_msg, sender, history):
    try:
        if needs_search(incoming_msg):
            response = claude.beta.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=512,
                system="You are Safa7, a concise personal assistant. Search the web and give a SHORT answer — 2-3 sentences max. Just the key fact.",
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": incoming_msg}],
                betas=["web-search-2025-03-05"]
            )
        else:
            response = claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=512,
                system="You are Safa7, a personal AI assistant. Adapt your style to the topic — casual for simple things, professional for important matters. Remember everything the user tells you. Be concise.",
                messages=history
            )

        reply = ""
        for block in response.content:
            if getattr(block, "type", "") == "text":
                reply += block.text

        if not reply:
            reply = "Sorry, I couldn't get that. Try asking again."

        history.append({"role": "assistant", "content": reply})
        if len(history) > 30:
            history = history[-30:]
        save_memory(sender, history)

        twilio = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
        twilio.messages.create(
            body=reply,
            from_=TWILIO_NUMBER,
            to=sender
        )

    except Exception as e:
        print(f"Error in background thread: {e}")

@app.route("/webhook", methods=["POST"])
def webhook():
    incoming_msg = request.values.get("Body", "").strip()
    sender = request.values.get("From", "")

    memory = load_memory()
    history = memory.get(sender, [])
    history.append({"role": "user", "content": incoming_msg})

    # Start processing in background
    thread = threading.Thread(target=process_and_reply, args=(incoming_msg, sender, history))
    thread.start()

    # Immediately respond to Twilio to avoid timeout
    return "", 204

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
