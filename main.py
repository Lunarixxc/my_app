# main.py
import os
import re
import json
import hashlib
from datetime import datetime
from typing import Optional, List, Dict
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
    description="Трекинг расходов с персистентной копилкой и ручным вводом.",
    version="3.1.0"
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
STATE_SHEET_NAME = "State"
HEADER = ["id", "ts_utc", "ts_msk", "amount", "currency", "type", "description", "balance_after", "source_msg"]

def get_sheets_service():
    creds = Credentials.from_service_account_info(GOOGLE_SA_INFO, scopes=SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds)

def read_state() -> Dict[str, str]:
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"{STATE_SHEET_NAME}!A:B").execute()
        rows = result.get("values", [])
        return {row[0]: row[1] for row in rows[1:] if len(row) == 2}
    except HttpError: return {}

def update_state_value(key: str, value: any):
    service = get_sheets_service()
    body = {'values': [[str(value)]]}
    state_data = read_all_rows(STATE_SHEET_NAME)
    row_index = -1
    for i, row in enumerate(state_data):
        if row and row[0] == key: row_index = i + 1; break
    if row_index != -1:
        service.spreadsheets().values().update(spreadsheetId=GOOGLE_SHEET_ID, range=f"{STATE_SHEET_NAME}!B{row_index}", valueInputOption="RAW", body=body).execute()

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

# --- Утилиты и парсеры ---
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

# --- РЕФАКТОРИНГ: Единая функция обработки расхода ---
def process_debit_transaction(amount: float, description: str, source_msg: str = "") -> Dict:
    """Обрабатывает транзакцию расхода, обновляет состояние и возвращает статистику."""
    ts_utc = datetime.now(pytz.UTC)
    ts_msk = ts_utc.astimezone(MOSCOW_TZ)
    msg_id = make_id(source_msg or f"manual_{amount}", ts_utc.isoformat())

    # 1. Обновляем состояние копилки
    current_state = read_state()
    piggy_bank = float(current_state.get('piggy_bank', 0.0))
    new_piggy_bank = piggy_bank - amount
    update_state_value('piggy_bank', new_piggy_bank)

    # 2. Записываем транзакцию
    all_rows = read_all_rows(SHEET_NAME)
    if not all_rows: append_row(HEADER)
    new_row = [msg_id, ts_utc.isoformat(), ts_msk.isoformat(), amount, "RUB", "debit", description, None, source_msg]
    append_row(new_row)

    # 3. Считаем статистику для ответа
    spent_today = 0.0
    today_date = ts_msk.date()
    data_rows = (all_rows[1:] if all_rows else []) + [new_row]
    for row in data_rows:
        try:
            row_ts = datetime.fromisoformat(row[2])
            if row_ts.date() == today_date and row[5] == 'debit':
                spent_today += float(row[3])
        except (ValueError, IndexError): continue
    
    daily_limit_left = DAILY_SPEND_LIMIT - spent_today
    overspent_monthly = max(0, -new_piggy_bank)

    return {"daily_limit_left": daily_limit_left, "overspent_monthly": overspent_monthly}

# --- Эндпоинты API ---
class IncomingSms(BaseModel):
    body: str
    time: Optional[str] = None

@app.post("/sms")
async def process_sms(payload: IncomingSms):
    parsed = parse_message(payload.body)
    if parsed.get("amount") is None:
        raise HTTPException(status_code=400, detail="Could not parse amount from message body.")
    
    amount = parsed["amount"]
    
    if parsed["type"] == "debit":
        stats = process_debit_transaction(amount, parsed["description"], payload.body.strip())
        text = (
            f"<b>Расход:</b> {amount} ₽\n\n"
            f"<b>Остаток на сегодня:</b> {stats['daily_limit_left']:+.2f} ₽\n"
            f"<b>Потрачено за месяц лишнего:</b> {stats['overspent_monthly']:.2f} ₽"
        )
        await send_telegram(text)
    # TODO: можно добавить обработку для `credit` (пополнений) в будущем
    
    return {"status": "ok"}

@app.post(f"/telegram/webhook/{TG_SECRET_PATH}", include_in_schema=False)
async def tg_webhook(update: Dict):
    message = update.get("message") or update.get("edited_message")
    if not message or str(message.get("chat", {}).get("id")) != TELEGRAM_CHAT_ID:
        return {"ok": True}

    text = message.get("text", "").strip()
    command = text.lower() # для сравнения команд
    
    if command in ("/start", "/help"):
        await send_telegram(
            "Привет! Я твой финансовый бот.\n"
            "<b>Доступные команды:</b>\n"
            "/status - Показать текущий бюджет.\n"
            "/add <b>сумма</b> - Ручной ввод расхода (например, <code>/add 150.50</code>).\n"
            "/cancel - Отменить последнюю транзакцию."
        )
    elif command == "/status":
        state = read_state()
        piggy_bank = float(state.get('piggy_bank', 0.0))
        overspent_monthly = max(0, -piggy_bank)
        report = (
            f"<b>Текущий статус:</b>\n\n"
            f"Дневной лимит: {DAILY_SPEND_LIMIT} ₽\n"
            f"Потрачено за месяц лишнего: {overspent_monthly:.2f} ₽\n"
            f"Сэкономлено (в копилке): {max(0, piggy_bank):.2f} ₽"
        )
        await send_telegram(report)
        
    elif command.startswith("/add "):
        try:
            amount_str = text.split(" ", 1)[1]
            amount = float(amount_str)
            if amount <= 0:
                await send_telegram("Сумма должна быть положительным числом.")
                return {"ok": True}
            
            # Используем нашу новую централизованную функцию
            stats = process_debit_transaction(amount, "Ручной ввод", f"Manual entry via /add")
            
            # Отправляем такое же сообщение, как и при SMS
            response_text = (
                f"<b>Расход (вручную):</b> {amount} ₽\n\n"
                f"<b>Остаток на сегодня:</b> {stats['daily_limit_left']:+.2f} ₽\n"
                f"<b>Потрачено за месяц лишнего:</b> {stats['overspent_monthly']:.2f} ₽"
            )
            await send_telegram(response_text)

        except (ValueError, IndexError):
            await send_telegram("Неверный формат. Используйте: <code>/add СУММА</code> (например, <code>/add 500</code>)")

    elif command == "/cancel":
        all_rows = read_all_rows(SHEET_NAME)
        if len(all_rows) < 2:
            await send_telegram("Нет транзакций для отмены."); return {"ok": True}
        
        last_transaction = all_rows[-1]
        try:
            amount_to_revert = float(last_transaction[3]); transaction_type = last_transaction[5]; description = last_transaction[6]
        except (ValueError, IndexError):
            await send_telegram("Ошибка: не удалось прочитать последнюю транзакцию."); return {"ok": True}
            
        state = read_state(); piggy_bank = float(state.get('piggy_bank', 0.0))
        new_piggy_bank = piggy_bank + amount_to_revert if transaction_type == "debit" else piggy_bank - amount_to_revert
        update_state_value('piggy_bank', new_piggy_bank)
        
        delete_last_row()
        await send_telegram(f"✅ Последняя транзакция ({description} на {amount_to_revert} ₽) отменена.")

    return {"ok": True}

@app.get("/", summary="Статус сервиса")
def read_root():
    return {"status": "ok", "version": "3.1.0"}
