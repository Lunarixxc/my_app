import hashlib
import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
import httpx

from pydantic import BaseModel

# === ENV ===
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]  # твой личный chat_id
GOOGLE_SA_JSON = os.environ["GOOGLE_SA_JSON"]      # сервисный аккаунт (JSON строкой)
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
ALLOWED_SENDERS = os.environ.get("ALLOWED_SENDERS", "900,9000,9009,SBERBANK").split(",")

app = FastAPI()

# === Utils ===

def make_id(sender: str, body: str, ts: str) -> str:
    raw = f"{sender}|{body}|{ts}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]

def parse_amount(text: str) -> Optional[float]:
    # грубый пример: ищем число с разделителями, валюту — отдельно
    import re
    m = re.search(r"(\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d{1,2})?)", text)
    if not m: 
        return None
    val = m.group(1).replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(val)
    except:
        return None

def detect_currency(text: str) -> str:
    t = text.lower()
    if "₽" in t or "rub" in t or "р" in t:
        return "RUB"
    if "€" in t or "eur" in t:
        return "EUR"
    if "$" in t or "usd" in t:
        return "USD"
    return "RUB"

def parse_sber(text: str) -> dict:
    import re
    d = {
        "type": None,
        "amount": parse_amount(text),
        "currency": detect_currency(text),
        "card_mask": None,
        "merchant": None,
        "balance_after": None,
    }
    # тип
    if re.search(r"покупка|списание|оплата", text, re.I):
        d["type"] = "debit"
    elif re.search(r"зачислен|пополнение|возврат", text, re.I):
        d["type"] = "credit"
    # карта
    m = re.search(r"карта\s*[*#xX]?(\d{4})", text, re.I)
    if m: d["card_mask"] = f"*{m.group(1)}"
    # мерчант
    m = re.search(r"(?:магазин|получатель|перевод|atm|терминал)\s*[:\-]?\s*([\w\d\-\s\.\,]+)", text, re.I)
    if m: d["merchant"] = m.group(1).strip()[:80]
    # баланс
    m = re.search(r"(?:баланс|доступно)[:\s]*([\d \u00A0\.,]+)", text, re.I)
    if m:
        try:
            bal = m.group(1).replace(" ", "").replace("\u00A0","").replace(",", ".")
            d["balance_after"] = float(bal)
        except:
            pass
    return d

async def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        })

# === Google Sheets: простая запись ===
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

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
        range="Transactions!A1:Z99999"
    ).execute()
    return res.get("values", [])

# === Схема входа от форвардера ===

class SmsPayload(BaseModel):
    sender: str
    body: str
    received_at: Optional[str] = None  # ISO; если нет — возьмём сейчас

@app.post("/sms")
async def sms_webhook(payload: SmsPayload, request: Request):
    sender = (payload.sender or "").strip()
    if sender not in ALLOWED_SENDERS:
        raise HTTPException(403, "sender not allowed")

    ts = payload.received_at or datetime.now(timezone.utc).isoformat()
    msg_id = make_id(sender, payload.body, ts)
    parsed = parse_sber(payload.body)

    # идемпотентность: если такой id уже есть — выходим
    rows = read_all()
    headers = rows[0] if rows else []
    existing_ids = {r[0] for r in rows[1:]} if rows else set()
    if msg_id in existing_ids:
        return {"status":"duplicate_skipped"}

    row = [
        msg_id, ts, parsed.get("amount"), parsed.get("currency"),
        parsed.get("type"), parsed.get("card_mask"),
        parsed.get("merchant"), parsed.get("balance_after"),
        sender, payload.body.strip()
    ]
    # если таблица пустая — добавим заголовки
    if not rows:
        append_row(["id","ts","amount","currency","type","card_mask","merchant","balance_after","source","raw_text"])
    append_row(row)

    # простая агрегатика по дню/неделе
    now = datetime.now(timezone.utc)
    total_day = 0.0
    total_week = 0.0
    from datetime import timedelta
    start_day = now.replace(hour=0,minute=0,second=0,microsecond=0)
    start_week = (now - timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)

    for r in rows[1:]:
        try:
            _ts = datetime.fromisoformat(r[1])
            _amt = float(r[2]) if r[2] else 0.0
            _type = r[4]
            signed = -_amt if _type == "debit" else _amt
            if _ts >= start_day: total_day += signed
            if _ts >= start_week: total_week += signed
        except Exception:
            pass
    # учтём текущую
    cur_signed = -(parsed["amount"] or 0.0) if parsed.get("type")=="debit" else (parsed["amount"] or 0.0)
    if datetime.fromisoformat(ts) >= start_day: total_day += cur_signed
    if datetime.fromisoformat(ts) >= start_week: total_week += cur_signed

    # сообщение в TG
    amt = parsed.get("amount")
    cur = parsed.get("currency","RUB")
    tpe = parsed.get("type") or "transaction"
    card = parsed.get("card_mask") or "—"
    merch = parsed.get("merchant") or "—"
    bal = parsed.get("balance_after")

    text = (
        f"<b>{'−' if tpe=='debit' else '+'}{amt or '?'} {cur}</b>\n"
        f"Тип: {tpe}\n"
        f"Карта: {card}\n"
        f"Мерчант: {merch}\n"
        f"Баланс: {bal if bal is not None else '—'}\n\n"
        f"Итого сегодня: {round(total_day,2)} {cur}\n"
        f"Итого за неделю: {round(total_week,2)} {cur}"
    )
    await send_telegram(text)
    return {"status":"ok","id":msg_id}
