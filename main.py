# main.py
import os
import re
import json
import hashlib
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import pytz  # Библиотека для работы с часовыми поясами

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import httpx

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Конфигурация приложения ---
app = FastAPI(
    title="Personal Finance Bot",
    description="Трекинг расходов с дневным бюджетом и умной копилкой.",
    version="2.2.0"
)

# --- Переменные окружения ---
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
google_sa_json_str = os.environ["GOOGLE_SA_JSON"]
TG_SECRET_PATH = os.environ.get("TG_SECRET_PATH", "super-secret-path-123")

# --- Константы бюджета и времени ---
MONTHLY_INCOME = 69600.0
MONTHLY_SAVINGS_GOAL = 20000.0
MONTHLY_SPEND_BUDGET = MONTHLY_INCOME - MONTHLY_SAVINGS_GOAL
AVG_DAYS_IN_MONTH = 30.4375
DAILY_SPEND_LIMIT = round(MONTHLY_SPEND_BUDGET / AVG_DAYS_IN_MONTH, 2)
MOSCOW_TZ = pytz.timezone('Europe/Moscow')  # Указываем наш часовой пояс

# --- Безопасная загрузка JSON ---
try:
    GOOGLE_SA_INFO = json.loads(google_sa_json_str)
except json.JSONDecodeError:
    import base64
    GOOGLE_SA_INFO = json.loads(base64.b64decode(google_sa_json_str))

# --- Google Sheets API ---
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Transactions"
HEADER = ["id", "ts_utc", "ts_msk", "amount", "currency", "type", "description", "balance_after", "source_msg"]

def get_sheets_service():
    creds = Credentials.from_service_account_info(GOOGLE_SA_INFO, scopes=SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds)

def read_all_rows() -> List[List[str]]:
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{SHEET_NAME}!A:I"
        ).execute()
        return result.get("values", [])
    except HttpError:
        return []

def append_row(row: list):
    try:
        service = get_sheets_service()
        service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [row]}
        ).execute()
    except HttpError as error:
        print(f"Error appending row: {error}")
        raise

# --- Telegram API ---
async def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            await client.post(url, json=payload)
        except httpx.RequestError as e:
            print(f"Error sending to Telegram: {e}")

# --- Парсер и утилиты ---
def make_id(body: str, ts: str) -> str:
    raw = f"{body}|{ts}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]

def parse_amount(text: str) -> Optional[float]:
    match = re.search(r"(\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d{1,2})?)\s*₽", text)
    if not match: return None
    value_str = match.group(1).replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return None

def parse_message(text: str) -> dict:
    data = {
        "type": "debit",
        "amount": parse_amount(text),
        "currency": "RUB",
        "description": "",
        "balance_after": None,
    }
    
    patterns = [
        r"Покупка на .*?, (.*?)(?=Доступно|Баланс|$)",
        r"Оплата через СБП на .*?, (.*?)(?=Доступно|Баланс|$)",
        r"Перевод на .*?\. (.*?)\.",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            data["description"] = match.group(1).strip().rstrip('.').strip()
            break

    if not data["description"]:
        data["description"] = text.splitlines()[0]

    if re.search(r"зачислен|пополнение|возврат|зарплата", text, re.I):
        data["type"] = "credit"
        
    match = re.search(r"(?:Доступно|Баланс)\s*([\d\s\u00A0,.]+)₽", text, re.I)
    if match:
        data["balance_after"] = parse_amount(match.group(1) + " ₽")

    return data

# --- Логика бюджета ---
def calculate_budget_stats(all_rows: List[List[str]]) -> dict:
    now_msk = datetime.now(MOSCOW_TZ)
    today_start_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    
    spent_today = 0.0
    data_rows = all_rows[1:] if all_rows and all_rows[0] == HEADER else all_rows

    if not data_rows:
        return {
            "spent_today": 0.0,
            "daily_limit_left": DAILY_SPEND_LIMIT,
            "smart_piggy_bank": 0.0
        }

    first_transaction_ts_str = data_rows[0][2]
    first_transaction_date = datetime.fromisoformat(first_transaction_ts_str).date()
    
    total_spent_since_start = 0.0
    for row in data_rows:
        try:
            ts_msk_str, amount_str, type_str = row[2], row[3], row[5]
            if type_str != "debit": continue
            
            ts_msk = datetime.fromisoformat(ts_msk_str)
            amount = float(amount_str)
            
            total_spent_since_start += amount
            if ts_msk >= today_start_msk:
                spent_today += amount
        except (ValueError, IndexError, TypeError):
            continue
            
    days_since_start = (now_msk.date() - first_transaction_date).days
    planned_spend_to_date = days_since_start * DAILY_SPEND_LIMIT
    smart_piggy_bank = planned_spend_to_date - total_spent_since_start

    return {
        "spent_today": round(spent_today, 2),
        "daily_limit_left": round(DAILY_SPEND_LIMIT - spent_today, 2),
        "smart_piggy_bank": round(smart_piggy_bank, 2)
    }

# --- Новая функция для парсинга времени ---
def parse_flexible_time(time_str: str) -> datetime:
    """Пытается распознать время из нескольких возможных форматов."""
    time_str = time_str.replace('\u202f', ' ')
    # Формат 1: '11.08, 9:49 PM'
    try:
        dt_obj = datetime.strptime(f"{datetime.now().year} {time_str}", '%Y %d.%m, %I:%M %p')
        # Считаем, что пришедшее время уже в нашем целевом часовом поясе (Московском)
        # и затем локализуем его, чтобы можно было конвертировать в UTC
        localized_dt = MOSCOW_TZ.localize(dt_obj)
        return localized_dt.astimezone(pytz.UTC)
    except ValueError:
        pass

    # Формат 2: Стандартный ISO 8601
    try:
        return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except ValueError:
        pass

    # Fallback: если ничего не подошло
    return datetime.now(pytz.UTC)

# --- Эндпоинты API ---
class IncomingSms(BaseModel):
    body: str
    time: Optional[str] = None

@app.post("/sms")
async def process_sms(payload: IncomingSms):
    ts_str = payload.time if payload.time else datetime.now(pytz.UTC).isoformat()
    ts_utc = parse_flexible_time(ts_str)
    ts_msk = ts_utc.astimezone(MOSCOW_TZ)
    
    msg_id = make_id(payload.body, ts_utc.isoformat())

    all_rows = read_all_rows()
    data_rows = all_rows[1:] if all_rows and all_rows[0] == HEADER else all_rows
    if any(msg_id == row[0] for row in data_rows if row):
        return {"status": "duplicate", "id": msg_id}

    parsed = parse_message(payload.body)
    if parsed.get("amount") is None:
        raise HTTPException(status_code=400, detail="Could not parse amount from message body.")

    new_row = [
        msg_id,
        ts_utc.isoformat(),
        ts_msk.isoformat(),
        parsed["amount"],
        parsed["currency"],
        parsed["type"],
        parsed["description"],
        parsed["balance_after"],
        payload.body.strip()
    ]
    if not all_rows:
        append_row(HEADER)
    append_row(new_row)

    if parsed["type"] == "debit":
        stats = calculate_budget_stats(data_rows + [new_row])
        limit_left = stats['daily_limit_left']
        piggy_bank = stats['smart_piggy_bank']
        emoji_status = "✅" if limit_left >= 0 else "⚠️"
        
        text = (
            f"<b>Расход: {parsed['amount']} {parsed['currency']}</b> ({parsed['description']})\n\n"
            f"{emoji_status} Остаток на сегодня: <b>{limit_left:+.2f} ₽</b>\n"
            f"🐷 Копилка месяца: <code>{piggy_bank:+.2f} ₽</code>\n"
            f"💰 Баланс карты: {parsed['balance_after'] or 'N/A'} ₽"
        )
        await send_telegram(text)

    return {"status": "ok", "id": msg_id}

@app.post(f"/telegram/webhook/{TG_SECRET_PATH}", include_in_schema=False)
async def tg_webhook(update: Dict):
    message = update.get("message") or update.get("edited_message")
    if not message or str(message.get("chat", {}).get("id")) != TELEGRAM_CHAT_ID:
        return {"ok": True}

    text = message.get("text", "").strip().lower()
    
    if text in ("/start", "/help"):
        await send_telegram(
            "Привет! Я твой финансовый бот.\n"
            "Я автоматически считаю твои расходы и дневной бюджет.\n\n"
            "<b>Доступные команды:</b>\n"
            "/status - Показать текущий бюджет на день и состояние копилки.\n"
            f"Твой дневной лимит: <b>{DAILY_SPEND_LIMIT} ₽</b>"
        )
    elif text == "/status":
        all_rows = read_all_rows()
        stats = calculate_budget_stats(all_rows)
        limit_left = stats['daily_limit_left']
        piggy_bank = stats['smart_piggy_bank']
        
        report = (
            f"<b>Статус на сегодня:</b>\n\n"
            f"Дневной лимит: {DAILY_SPEND_LIMIT} ₽\n"
            f"Потрачено сегодня: {stats['spent_today']} ₽\n"
            f"Остаток на сегодня: <b>{limit_left:+.2f} ₽</b>\n\n"
            f"🐷 Копилка месяца: <code>{piggy_bank:+.2f} ₽</code>\n"
            f"<i>(положительна, если тратишь меньше плана)</i>"
        )
        await send_telegram(report)
        
    return {"ok": True}

@app.get("/", summary="Статус сервиса")
def read_root():
    return {"status": "ok", "version": "2.2.0"}
