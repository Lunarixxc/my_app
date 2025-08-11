# main.py
import os
import re
import json
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict

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
    version="2.0.0"
)

# --- Переменные окружения ---
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
google_sa_json_str = os.environ["GOOGLE_SA_JSON"]
TG_SECRET_PATH = os.environ.get("TG_SECRET_PATH", "super-secret-path-123")

# --- Константы бюджета ---
MONTHLY_INCOME = 69600.0
MONTHLY_SAVINGS_GOAL = 20000.0
MONTHLY_SPEND_BUDGET = MONTHLY_INCOME - MONTHLY_SAVINGS_GOAL
AVG_DAYS_IN_MONTH = 30.4375 # Более точное среднее
DAILY_SPEND_LIMIT = round(MONTHLY_SPEND_BUDGET / AVG_DAYS_IN_MONTH, 2) # ~1630 ₽

# --- Безопасная загрузка JSON из переменной окружения ---
try:
    GOOGLE_SA_INFO = json.loads(google_sa_json_str)
except json.JSONDecodeError:
    import base64
    GOOGLE_SA_INFO = json.loads(base64.b64decode(google_sa_json_str))

# --- Google Sheets API ---
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Transactions"
HEADER = ["id", "ts", "amount", "currency", "type", "description", "balance_after", "source_msg"]

def get_sheets_service():
    creds = Credentials.from_service_account_info(GOOGLE_SA_INFO, scopes=SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds)

def read_all_rows() -> List[List[str]]:
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{SHEET_NAME}!A:H"
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
        raise HTTPException(status_code=500, detail="Failed to write to Google Sheet.")

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
    # Ищем числа с разделителями (пробел, неразрывный пробел) и возможными копейками (через точку или запятую)
    match = re.search(r"(\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d{1,2})?)\s*₽", text)
    if not match:
        return None
    value_str = match.group(1).replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return None

def parse_message(text: str) -> dict:
    data = {
        "type": "debit", # По умолчанию считаем все расходом
        "amount": parse_amount(text),
        "currency": "RUB",
        "description": "Не определено",
        "balance_after": None,
    }

    # Определяем тип операции
    if re.search(r"зачислен|пополнение|возврат|зарплата", text, re.I):
        data["type"] = "credit"
    
    # Извлекаем описание
    # Вариант 1: Покупка на X, [описание]
    match = re.search(r"Покупка на .*?, (.*)", text, re.I)
    if match:
        data["description"] = match.group(1).strip()
    
    # Вариант 2: Оплата через СБП ... [описание]
    match = re.search(r"Оплата через СБП на .*?, (.*)", text, re.I)
    if match:
        data["description"] = f"СБП: {match.group(1).strip()}"
        
    # Вариант 3: Перевод на X. [Имя]
    match = re.search(r"Перевод на .*?\. (.*?)\.", text, re.I)
    if match:
        data["description"] = f"Перевод: {match.group(1).strip()}"
    
    # Извлекаем баланс
    match = re.search(r"(?:Доступно|Баланс)\s*([\d\s\u00A0,.]+)₽", text, re.I)
    if match:
        data["balance_after"] = parse_amount(match.group(1) + " ₽") # Добавляем рубль для унификации

    return data


# --- Логика бюджета ---
def calculate_budget_stats(all_rows: List[List[str]]) -> dict:
    """Считает статистику по бюджету за сегодня и за месяц."""
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    spent_today = 0.0
    spent_this_month = 0.0
    
    data_rows = all_rows[1:] if all_rows and all_rows[0] == HEADER else all_rows

    for row in data_rows:
        try:
            ts_str, amount_str, type_str = row[1], row[2], row[4]
            if type_str != "debit": continue # Учитываем только расходы
            
            ts = datetime.fromisoformat(ts_str)
            amount = float(amount_str)

            if ts >= today_start:
                spent_today += amount
            
            if ts >= month_start:
                spent_this_month += amount
        except (ValueError, IndexError, TypeError):
            continue
            
    # Расчет "умной копилки"
    days_passed_in_month = (now - month_start).days
    planned_spend_to_date = days_passed_in_month * DAILY_SPEND_LIMIT
    smart_piggy_bank = planned_spend_to_date - spent_this_month

    return {
        "spent_today": round(spent_today, 2),
        "daily_limit_left": round(DAILY_SPEND_LIMIT - spent_today, 2),
        "smart_piggy_bank": round(smart_piggy_bank, 2)
    }


# --- Эндпоинты API ---

class IncomingSms(BaseModel):
    body: str
    time: Optional[str] = None

@app.post("/sms", summary="Обработка входящего сообщения о транзакции")
async def process_sms(payload: IncomingSms):
    # Убираем фильтр по отправителю, как ты и просил
    ts = payload.time or datetime.now(timezone.utc).isoformat()
    msg_id = make_id(payload.body, ts)

    all_rows = read_all_rows()
    data_rows = all_rows[1:] if all_rows and all_rows[0] == HEADER else all_rows
    existing_ids = {row[0] for row in data_rows if row}
    if msg_id in existing_ids:
        return {"status": "duplicate", "id": msg_id}

    parsed = parse_message(payload.body)
    if parsed.get("amount") is None:
        raise HTTPException(status_code=400, detail="Could not parse amount from message body.")

    # Добавляем в таблицу
    new_row = [
        msg_id, ts, parsed["amount"], parsed["currency"],
        parsed["type"], parsed["description"],
        parsed["balance_after"], payload.body.strip()
    ]
    if not all_rows:
        append_row(HEADER)
    append_row(new_row)

    # Уведомление только о расходах
    if parsed["type"] == "debit":
        # Пересчитываем статистику, включая только что добавленную транзакцию
        stats = calculate_budget_stats(all_rows + [new_row])
        
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
    return {"status": "ok", "version": "2.0.0"}
