import os
import json
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import anthropic

app = Flask(__name__)
client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")

def get_sheet():
    creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh.sheet1

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

@app.route("/webhook", methods=["POST"])
def webhook():
    incoming_msg = request.values.get("Body", "").strip()
    sender = request.values.get("From", "")

    memory = load_memory()
    history = memory.get(sender, [])

    history.append({"role": "user", "content": incoming_msg})

    response = client.beta.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        system="""You are Safa7, a personal AI assistant. You adapt your communication style to the topic — casual and concise for simple things, professional and detailed for important matters. You have a persistent memory and remember everything the user tells you across all conversations. When you need current information, news, prices, or anything that requires up-to-date data, use your web search tool. Always provide a text answer after searching. Be proactive, efficient, and helpful.""",
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=history,
        betas=["web-search-2025-03-05"]
    )

    # Extract all text blocks from response
    reply = ""
    for block in response.content:
        block_type = getattr(block, "type", "")
        if block_type == "text":
            reply += block.text

    if not reply:
        reply = "I found some information but had trouble formatting it. Please try asking again."

    print(f"REPLY: {reply[:100]}", flush=True)

    history.append({"role": "assistant", "content": reply})

    if len(history) > 50:
        history = history[-50:]

    save_memory(sender, history)

    resp = MessagingResponse()
    resp.message(reply)
    return str(resp)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
