import os
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import anthropic

app = Flask(__name__)
client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
conversation_history = {}

@app.route("/webhook", methods=["POST"])
def webhook():
    import sys
       print(f"API KEY STARTS WITH: {os.environ.get('ANTHROPIC_API_KEY', 'NOT FOUND')[:20]}", file=sys.stderr)
    incoming_msg = request.values.get("Body", "").strip()
    sender = request.values.get("From", "")
    
    if sender not in conversation_history:
        conversation_history[sender] = []
    
    conversation_history[sender].append({
        "role": "user",
        "content": incoming_msg
    })
    
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system="You are a personal assistant. You help with tasks, research, notes, and reminders. Be concise and helpful.",
        messages=conversation_history[sender]
    )
    
    reply = response.content[0].text
    
    conversation_history[sender].append({
        "role": "assistant",
        "content": reply
    })
    
    resp = MessagingResponse()
    resp.message(reply)
    return str(resp)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
