# main.py
import os
import re
import json
import hashlib
from datetime import datetime
from typing import Optional, List, Dict
from collections import defaultdict
import pytz

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Конфигурация приложения ---
app = FastAPI(
    title="Personal Finance Bot",
    description="Трекинг расходов с корректной логикой перерасхода.",
    version="3.2.0"
)

# --- Переменные окружения и константы ---
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
google_sa_json_str = os.environ["GOOGLE_SA_JSON"]
TG_SECRET_PATH = os.environ.get("TG_SECRET_PATH", "super-secret-path-123")

MONTHLY_INCOME = 69600.0
MONTHLY_SAVINGS_GOAL = 20000.0
MONTHLY_SPEND_BUDGET = MONTHLY_INCOME - MONTHLY_SAVINGS_GOAL
AVG_DAYS_IN_MONTH = 30.4375
DAILY_SPEND_LIMIT = round(MONTHLY_SPEND_BUDGET / AVG_DAYS_IN_MONTH, 2)
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# --- Google Sheets ---
try:
    GOOGLE_SA_INFO = json.loads(google_sa_json_str)
except json.JSONDecodeError:
    import base64
    GOOGLE_SA_INFO = json.loads(base64.b64decode(google_sa_json_str))
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Transactions"
HEADER = ["id", "ts_utc", "ts_msk", "amount", "currency", "type", "description", "balance_after", "source_msg"]

def get_sheets_service():
    creds = Credentials.from_service_account_info(GOOGLE_SA_INFO, scopes=SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds)

def read_all_rows(sheet_name: str) -> List[List[str]]:
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"{sheet_name}!A:Z").execute()
        return result.get("values", [])
    except HttpError: return []

def append_row(row: list):
    service = get_sheets_service()
    service.spreadsheets().values().append(spreadsheetId=GOOGLE_SHEET_ID, range=f"{SHEET_NAME}!A1", valueInputOption="USER_ENTERED", body={"values": [row]}).execute()

def delete_last_row():
    service = get_sheets_service()
    rows = read_all_rows(SHEET_NAME)
    if len(rows) < 2: return
    last_row_index = len(rows)
    service.spreadsheets().values().clear(spreadsheetId=GOOGLE_SHEET_ID, range=f"{SHEET_NAME}!A{last_row_index}:Z{last_row_index}", body={}).execute()

async def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"; payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    async with httpx.AsyncClient(timeout=10) as client:
        try: await client.post(url, json=payload)
        except httpx.RequestError as e: print(f"Error sending to Telegram: {e}")

# --- Утилиты и парсеры (без изменений) ---
def parse_amount(text: str):
    match = re.search(r"(\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d{1,2})?)\s*₽", text)
    if not match: return None
    value_str = match.group(1).replace(" ", "").replace("\u00A0", "").replace(",", ".");
    try: return float(value_str)
    except (ValueError, TypeError): return None
def parse_message(text: str):
    data = {"type": "debit", "amount": parse_amount(text), "currency": "RUB", "description": "", "balance_after": None}
    patterns = [r"Покупка на .*?, (.*?)(?=Доступно|Баланс|$)", r"Оплата через СБП на .*?, (.*?)(?=Доступно|Баланс|$)", r"Перевод на .*?\. (.*?)\."]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match: data["description"] = match.group(1).strip().rstrip('.').strip(); break
    if not data["description"]: data["description"] = text.splitlines()[0]
    if re.search(r"зачислен|пополнение|возврат|зарплата", text, re.I): data["type"] = "credit"
    match = re.search(r"(?:Доступно|Баланс)\s*([\d\s\u00A0,.]+)₽", text, re.I)
    if match: data["balance_after"] = parse_amount(match.group(1) + " ₽")
    return data
def parse_flexible_time(time_str: str):
    time_str = time_str.replace('\u202f', ' ')
    try: dt_obj = datetime.strptime(f"{datetime.now().year} {time_str}", '%Y %d.%m, %I:%M %p'); localized_dt = MOSCOW_TZ.localize(dt_obj); return localized_dt.astimezone(pytz.UTC)
    except ValueError: pass
    try: return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except ValueError: pass
    return datetime.now(pytz.UTC)
def make_id(body: str, ts: str): raw = f"{body}|{ts}".encode("utf-8"); return hashlib.sha256(raw).hexdigest()[:16]

# --- НОВАЯ, ИСПРАВЛЕННАЯ ЛОГИКА БЮДЖЕТА ---
def calculate_budget_stats(all_rows: List[List[str]]) -> Dict:
    now_msk = datetime.now(MOSCOW_TZ)
    today_date = now_msk.date()
    
    data_rows = all_rows[1:] if all_rows and all_rows[0] == HEADER else all_rows
    
    daily_spends = defaultdict(float)
    for row in data_rows:
        try:
            ts_msk_str, amount_str, type_str = row[2], row[3], row[5]
            if type_str != "debit": continue
            ts_msk = datetime.fromisoformat(ts_msk_str)
            amount = float(amount_str)
            daily_spends[ts_msk.date()] += amount
        except (ValueError, IndexError): continue

    # Считаем накопленный результат (экономию/перерасход) за ВСЕ ПРОШЕДШИЕ дни
    cumulative_result = 0.0
    for day, total_spent in daily_spends.items():
        if day < today_date:
            cumulative_result += (DAILY_SPEND_LIMIT - total_spent)
            
    spent_today = daily_spends.get(today_date, 0.0)
    daily_limit_left = DAILY_SPEND_LIMIT - spent_today
    
    # "Лишние траты" - это отрицательная часть накопленного результата
    overspent_monthly = max(0, -cumulative_result)
    # "Сэкономлено" - это положительная часть
    savings = max(0, cumulative_result)

    return {
        "daily_limit_left": daily_limit_left,
        "overspent_monthly": overspent_monthly,
        "savings": savings,
        "spent_today": spent_today,
    }

# --- Общая функция для обработки транзакций ---
async def handle_transaction(amount: float, description: str, source_msg: str = ""):
    ts_utc = datetime.now(pytz.UTC)
    ts_msk = ts_utc.astimezone(MOSCOW_TZ)
    msg_id = make_id(source_msg or f"manual_{amount}", ts_utc.isoformat())

    all_rows = read_all_rows(SHEET_NAME)
    if not all_rows: append_row(HEADER)
    new_row = [msg_id, ts_utc.isoformat(), ts_msk.isoformat(), amount, "RUB", "debit", description, None, source_msg]
    append_row(new_row)
    
    # Считаем статистику, включая новую транзакцию
    stats = calculate_budget_stats(all_rows + [new_row])
    
    text = (
        f"<b>Расход:</b> {amount} ₽\n\n"
        f"<b>Остаток на сегодня:</b> {stats['daily_limit_left']:+.2f} ₽\n"
        f"<b>Потрачено за месяц лишнего:</b> {stats['overspent_monthly']:.2f} ₽"
    )
    await send_telegram(text)

# --- Эндпоинты API ---
class IncomingSms(BaseModel): body: str; time: Optional[str] = None

@app.post("/sms")
async def process_sms(payload: IncomingSms):
    parsed = parse_message(payload.body)
    if parsed.get("amount") is None:
        raise HTTPException(status_code=400, detail="Could not parse amount from message body.")
    if parsed["type"] == "debit":
        await handle_transaction(parsed["amount"], parsed["description"], payload.body.strip())
    return {"status": "ok"}

@app.post(f"/telegram/webhook/{TG_SECRET_PATH}", include_in_schema=False)
async def tg_webhook(update: Dict):
    message = update.get("message") or update.get("edited_message")
    if not message or str(message.get("chat", {}).get("id")) != TELEGRAM_CHAT_ID:
        return {"ok": True}

    text = message.get("text", "").strip()
    command = text.lower()
    
    if command in ("/start", "/help"):
        await send_telegram(
            "Привет! Я твой финансовый бот.\n"
            "<b>Доступные команды:</b>\n"
            "/status - Показать текущий бюджет.\n"
            "/add <b>сумма</b> - Ручной ввод расхода.\n"
            "/cancel - Отменить последнюю транзакцию."
        )
    elif command == "/status":
        all_rows = read_all_rows(SHEET_NAME)
        stats = calculate_budget_stats(all_rows)
        report = (
            f"<b>Текущий статус:</b>\n\n"
            f"Дневной лимит: {DAILY_SPEND_LIMIT} ₽\n"
            f"Потрачено сегодня: {stats['spent_today']:.2f} ₽\n"
            f"Остаток на сегодня: {stats['daily_limit_left']:+.2f} ₽\n\n"
            f"Потрачено за месяц лишнего: {stats['overspent_monthly']:.2f} ₽\n"
            f"Сэкономлено (в копилке): {stats['savings']:.2f} ₽"
        )
        await send_telegram(report)
        
    elif command.startswith("/add "):
        try:
            amount = float(text.split(" ", 1)[1])
            if amount <= 0: raise ValueError
            await handle_transaction(amount, "Ручной ввод")
        except (ValueError, IndexError):
            await send_telegram("Неверный формат. Используйте: <code>/add СУММА</code>")

    elif command == "/cancel":
        all_rows = read_all_rows(SHEET_NAME)
        if len(all_rows) < 2:
            await send_telegram("Нет транзакций для отмены."); return {"ok": True}
        
        last_transaction = all_rows[-1]
        description = last_transaction[6]
        amount_to_revert = float(last_transaction[3])
        
        delete_last_row()
        await send_telegram(f"✅ Последняя транзакция ({description} на {amount_to_revert} ₽) отменена.")

    return {"ok": True}

@app.get("/", summary="Статус сервиса")
def read_root(): return {"status": "ok", "version": "3.2.0"}
