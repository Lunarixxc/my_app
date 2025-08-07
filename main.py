import os
import re
import json
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import httpx

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

app = FastAPI()

# === ENV ===
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
GOOGLE_SA_JSON = os.environ["GOOGLE_SA_JSON"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
ALLOWED_SENDERS = os.environ.get("ALLOWED_SENDERS", "").split(",")
TG_SECRET_PATH = os.environ.get("TG_SECRET_PATH", "secret")

# === Google Sheets ===
def sheets_service():
    creds = Credentials.from_service_account_info(json.loads(GOOGLE_SA_JSON), scopes=[
        "https://www.googleapis.com/auth/spreadsheets"
    ])
    return build("sheets", "v4", credentials=creds)

def append_row(row: list):
    svc = sheets_service()
    svc.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range="Transactions!A1",
        valueInputOption="RAW",
        body={"values":[row]}
    ).execute()

def read_all():
    svc = sheets_service()
    res = svc.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEET_ID,
        range="Transactions!A1:Z9999"
    ).execute()
    return res.get("values", [])

# === Telegram ===
async def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        })

# === Utils ===
def make_id(sender: str, body: str, ts: str) -> str:
    raw = f"{sender}|{body}|{ts}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]

def parse_amount(text: str) -> Optional[float]:
    m = re.search(r"(\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d{1,2})?)", text)
    if not m: return None
    val = m.group(1).replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try: return float(val)
    except: return None

def detect_currency(text: str) -> str:
    t = text.lower()
    if "₽" in t or "rub" in t or "р" in t: return "RUB"
    if "€" in t or "eur" in t: return "EUR"
    if "$" in t or "usd" in t: return "USD"
    return "RUB"

def parse_sber(text: str) -> dict:
    d = {
        "type": None,
        "amount": parse_amount(text),
        "currency": detect_currency(text),
        "card_mask": None,
        "merchant": None,
        "balance_after": None,
    }
    if re.search(r"покупка|списание|оплата", text, re.I): d["type"] = "debit"
    elif re.search(r"зачислен|пополнение|возврат", text, re.I): d["type"] = "credit"
    m = re.search(r"карта\s*[*#xX]?(\d{4})", text, re.I)
    if m: d["card_mask"] = f"*{m.group(1)}"
    m = re.search(r"(?:магазин|получатель|перевод|atm|терминал)\s*[:\-]?\s*([\w\d\-\s\.\,]+)", text, re.I)
    if m: d["merchant"] = m.group(1).strip()[:80]
    m = re.search(r"(?:баланс|доступно)[:\s]*([\d \u00A0\.,]+)", text, re.I)
    if m:
        try:
            bal = m.group(1).replace(" ", "").replace("\u00A0","").replace(",", ".")
            d["balance_after"] = float(bal)
        except: pass
    return d

# === СМС эндпоинт ===
class SmsPayload(BaseModel):
    sender: str
    body: str
    received_at: Optional[str] = None

@app.post("/sms")
async def sms_webhook(payload: SmsPayload):
    sender = payload.sender.strip()
    if sender not in ALLOWED_SENDERS:
        raise HTTPException(403, "Sender not allowed")
    ts = payload.received_at or datetime.now(timezone.utc).isoformat()
    msg_id = make_id(sender, payload.body, ts)
    parsed = parse_sber(payload.body)

    rows = read_all()
    existing_ids = {r[0] for r in rows[1:]} if rows else set()
    if msg_id in existing_ids:
        return {"status":"duplicate"}

    if not rows:
        append_row(["id","ts","amount","currency","type","card_mask","merchant","balance_after","source","raw_text"])

    row = [
        msg_id, ts, parsed.get("amount"), parsed.get("currency"),
        parsed.get("type"), parsed.get("card_mask"),
        parsed.get("merchant"), parsed.get("balance_after"),
        sender, payload.body.strip()
    ]
    append_row(row)

    # расчёт итогов
    now = datetime.fromisoformat(ts)
    start_day = now.replace(hour=0,minute=0,second=0,microsecond=0)
    start_week = (now - timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)

    total_day = total_week = 0.0
    for r in rows[1:]:
        try:
            _ts = datetime.fromisoformat(r[1])
            _amt = float(r[2]) if r[2] else 0.0
            signed = -_amt if r[4]=="debit" else _amt
            if _ts >= start_day: total_day += signed
            if _ts >= start_week: total_week += signed
        except: pass

    cur_signed = -(parsed["amount"] or 0.0) if parsed.get("type")=="debit" else (parsed["amount"] or 0.0)
    if now >= start_day: total_day += cur_signed
    if now >= start_week: total_week += cur_signed

    text = (
        f"<b>{'−' if parsed['type']=='debit' else '+'}{parsed.get('amount')} {parsed.get('currency','RUB')}</b>\n"
        f"Тип: {parsed.get('type','?')}\n"
        f"Карта: {parsed.get('card_mask') or '—'}\n"
        f"Мерчант: {parsed.get('merchant') or '—'}\n"
        f"Баланс: {parsed.get('balance_after') or '—'}\n\n"
        f"Итого сегодня: {round(total_day,2)} {parsed.get('currency','RUB')}\n"
        f"Итого за неделю: {round(total_week,2)} {parsed.get('currency','RUB')}"
    )
    await send_telegram(text)
    return {"status":"ok","id":msg_id}

# === Telegram Webhook ===
@app.post(f"/telegram/webhook/{TG_SECRET_PATH}")
async def tg_webhook(update: dict):
    msg = update.get("message") or update.get("edited_message")
    if not msg: return {"ok":True}
    chat_id = msg["chat"]["id"]
    text = msg.get("text","").strip().lower()
    if str(chat_id) != TELEGRAM_CHAT_ID:
        return {"ok":True}

    if text in ("/start","/help"):
        await send_telegram("Доступные команды: /today, /week, /last")
    elif text == "/today":
        await send_telegram(await calc_report(1))
    elif text == "/week":
        await send_telegram(await calc_report(7))
    elif text == "/last":
        rows = read_all()
        last = rows[-1] if len(rows)>1 else "—"
        await send_telegram(f"Последняя запись:\n{last}")
    else:
        await send_telegram("Неизвестная команда. Используй /help")
    return {"ok":True}

async def calc_report(days: int) -> str:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    rows = read_all()
    total = cnt = 0
