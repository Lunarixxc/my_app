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
    title="SMS Finance Tracker",
    description="API для обработки SMS от банков, записи в Google Sheets и уведомлений в Telegram.",
    version="1.0.0"
)

# --- Переменные окружения ---
# Важно: эти переменные должны быть установлены в среде выполнения (например, на Render)
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
# Если переменная не задана, используем значение по умолчанию
ALLOWED_SENDERS = os.environ.get("ALLOWED_SENDERS", "").split(",")
TG_SECRET_PATH = os.environ.get("TG_SECRET_PATH", "super-secret-path-123")

# --- Безопасная загрузка JSON из переменной окружения ---
# Этот код будет работать как с однострочным JSON, так и с base64
google_sa_json_str = os.environ["GOOGLE_SA_JSON"]
try:
    # Пытаемся декодировать как JSON напрямую
    GOOGLE_SA_INFO = json.loads(google_sa_json_str)
except json.JSONDecodeError:
    # Если не вышло, пробуем как base64
    import base64
    GOOGLE_SA_INFO = json.loads(base64.b64decode(google_sa_json_str))

# --- Google Sheets API ---
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Transactions" # Имя листа в таблице

def get_sheets_service():
    """Создает и возвращает авторизованный сервис для работы с Google Sheets."""
    creds = Credentials.from_service_account_info(GOOGLE_SA_INFO, scopes=SHEETS_SCOPES)
    service = build("sheets", "v4", credentials=creds)
    return service

def read_all_rows() -> List[List[str]]:
    """Читает все строки с листа."""
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{SHEET_NAME}!A:J" # Читаем все 10 столбцов
        ).execute()
        return result.get("values", [])
    except HttpError as error:
        print(f"An error occurred: {error}")
        # В случае ошибки возвращаем пустой список, чтобы приложение не упало
        return []

def append_row(row: list):
    """Добавляет одну строку в конец таблицы."""
    try:
        service = get_sheets_service()
        service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED", # USER_ENTERED лучше для форматирования дат и чисел
            body={"values": [row]}
        ).execute()
    except HttpError as error:
        print(f"An error occurred while appending row: {error}")
        # Можно добавить отправку уведомления об ошибке в Telegram
        raise HTTPException(status_code=500, detail="Failed to write to Google Sheet.")

# --- Telegram API ---
async def send_telegram(text: str):
    """Отправляет сообщение в заданный Telegram чат."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            response = await client.post(url, json=payload)
            response.raise_for_status() # Проверка на HTTP ошибки (4xx, 5xx)
        except httpx.RequestError as e:
            print(f"Error sending message to Telegram: {e}")

# --- Утилиты и парсер ---
def make_id(sender: str, body: str, ts: str) -> str:
    """Создает уникальный ID для транзакции."""
    raw = f"{sender}|{body}|{ts}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]

def parse_amount(text: str) -> Optional[float]:
    """Извлекает сумму из текста. Улучшено для большей надежности."""
    # Ищем числа с разделителями пробелом, запятой или точкой
    match = re.search(r"(\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d{1,2})?)\s*(?:р|rub|₽|eur|€|usd|\$)", text, re.I)
    if not match:
        match = re.search(r"(\d+\.\d+|\d+)", text)
    if not match: return None
    
    # Нормализуем число: убираем пробелы, меняем запятую на точку
    value_str = match.group(1).replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return None

def detect_currency(text: str) -> str:
    """Определяет валюту."""
    t_lower = text.lower()
    if "₽" in t_lower or "rub" in t_lower or "р." in t_lower or "р " in t_lower: return "RUB"
    if "€" in t_lower or "eur" in t_lower: return "EUR"
    if "$" in t_lower or "usd" in t_lower: return "USD"
    return "RUB" # Валюта по умолчанию

def parse_sber(text: str) -> dict:
    """Парсит текст SMS от Сбера. Улучшен для обработки крайних случаев."""
    data = {
        "type": "undefined",
        "amount": parse_amount(text),
        "currency": detect_currency(text),
        "card_mask": None,
        "merchant": "Unknown",
        "balance_after": None,
    }

    if re.search(r"покупка|списание|оплата|перевод", text, re.I):
        data["type"] = "debit"
    elif re.search(r"зачислен|пополнение|возврат", text, re.I):
        data["type"] = "credit"

    # Карта
    m = re.search(r"(?:карта|счет|счёта)\s*[*#xX]+(\d{4})", text, re.I)
    if m: data["card_mask"] = f"*{m.group(1)}"

    # Мерчант (улучшенное регулярное выражение)
    # Ищем то, что идет после ключевых слов типа "покупка", "оплата" и до суммы
    m = re.search(r"(?:покупка|оплата|перевод)\s*(?:\d+[\.,]\d{2}р)?\s*([^.,\d]+?)\s*(?:Баланс|Доступно)", text, re.I)
    if m:
        # Убираем лишние пробелы и обрезаем
        data["merchant"] = re.sub(r'\s+', ' ', m.group(1).strip()).strip()[:80]
    
    # Баланс
    m = re.search(r"(?:баланс|доступно)[:\s]*([\d\s\u00A0.,]+)", text, re.I)
    if m:
        balance_val = parse_amount(m.group(1))
        if balance_val is not None:
            data["balance_after"] = balance_val
    
    return data

# --- Эндпоинт для входящих SMS ---
class SmsPayload(BaseModel):
    sender: str
    body: str
    received_at: Optional[str] = None

@app.post("/sms", summary="Обработка входящего SMS")
async def sms_webhook(payload: SmsPayload):
    sender = payload.sender.strip().upper()
    # Проверка на разрешенного отправителя (регистронезависимая)
    if not any(allowed.upper() in sender for allowed in ALLOWED_SENDERS if allowed):
        raise HTTPException(status_code=403, detail=f"Sender '{payload.sender}' not allowed.")

    ts = payload.received_at or datetime.now(timezone.utc).isoformat()
    msg_id = make_id(payload.sender, payload.body, ts)
    
    # РЕФАКТОРИНГ: Читаем все строки один раз для эффективности
    all_rows = read_all_rows()
    
    # Проверка на дубликат по ID
    # Пропускаем заголовок (первую строку), если он есть
    header = ["id", "ts", "amount", "currency", "type", "card_mask", "merchant", "balance_after", "source", "raw_text"]
    data_rows = all_rows[1:] if all_rows and all_rows[0] == header else all_rows
    existing_ids = {row[0] for row in data_rows if row}
    if msg_id in existing_ids:
        return {"status": "duplicate", "id": msg_id}

    # Парсим SMS
    parsed = parse_sber(payload.body)
    if parsed.get("amount") is None:
        raise HTTPException(status_code=400, detail="Could not parse amount from SMS body.")

    # Создаем строку для записи в таблицу
    new_row = [
        msg_id, ts, parsed["amount"], parsed["currency"],
        parsed["type"], parsed["card_mask"], parsed["merchant"],
        parsed["balance_after"], payload.sender, payload.body.strip()
    ]
    
    # Проверяем, есть ли заголовок, и добавляем его, если нужно
    if not all_rows:
        append_row(header)
    
    append_row(new_row)

    # Отправляем уведомление
    notification_text = (
        f"<b>{'Расход' if parsed['type'] == 'debit' else 'Доход'}: {parsed['amount']} {parsed['currency']}</b>\n"
        f"Тип: {parsed['merchant'] or 'N/A'}\n"
        f"Карта: {parsed['card_mask'] or 'N/A'}\n"
        f"Баланс: {parsed['balance_after'] or 'N/A'} {parsed['currency']}"
    )
    await send_telegram(notification_text)
    
    return {"status": "ok", "id": msg_id}

# --- Telegram Webhook для команд ---
async def calc_report(days: int) -> str:
    """ЗАВЕРШЕНА: Считает отчет по доходам и расходам за N дней."""
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=days)
    
    all_rows = read_all_rows()
    header = ["id", "ts", "amount", "currency", "type"] # Нам нужны только эти столбцы
    
    # Пропускаем заголовок
    data_rows = all_rows[1:] if all_rows and all_rows[0][0] == 'id' else all_rows

    total_debit = 0.0
    total_credit = 0.0
    transactions_count = 0

    for row in data_rows:
        try:
            # Валидация строки
            if len(row) < 5: continue
            
            # r[1] - ts, r[2] - amount, r[4] - type
            ts_str, amount_str, type_str = row[1], row[2], row[4]
            
            transaction_ts = datetime.fromisoformat(ts_str)
            if transaction_ts >= start_date:
                amount = float(amount_str)
                if type_str == "debit":
                    total_debit += amount
                elif type_str == "credit":
                    total_credit += amount
                transactions_count += 1
        except (ValueError, IndexError, TypeError):
            # Пропускаем строки с некорректным форматом
            continue
            
    total_net = total_credit - total_debit
    
    period_str = "сегодня" if days == 1 else f"последние {days} дней"
    if days == 7: period_str = "последнюю неделю"

    return (
        f"<b>Отчет за {period_str}:</b>\n\n"
        f"✅ Доходы: <code>{total_credit:.2f} RUB</code>\n"
        f"❌ Расходы: <code>{total_debit:.2f} RUB</code>\n"
        f"📈 Итог: <code>{total_net:+.2f} RUB</code>\n\n"
        f"Всего транзакций: {transactions_count}"
    )

@app.post(f"/telegram/webhook/{TG_SECRET_PATH}", include_in_schema=False)
async def tg_webhook(update: Dict):
    message = update.get("message") or update.get("edited_message")
    if not message:
        return {"ok": True}

    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "").strip().lower()

    if str(chat_id) != TELEGRAM_CHAT_ID:
        # Игнорируем сообщения не от нашего админа
        return {"ok": True}

    if text in ("/start", "/help"):
        await send_telegram("Привет! Я твой финансовый бот.\nДоступные команды:\n/today - отчет за сегодня\n/week - отчет за неделю\n/last30 - отчет за 30 дней")
    elif text == "/today":
        report = await calc_report(days=1)
        await send_telegram(report)
    elif text == "/week":
        report = await calc_report(days=7)
        await send_telegram(report)
    elif text == "/last30":
        report = await calc_report(days=30)
        await send_telegram(report)
    # Команду /last убрал, так как она не очень информативна, но можно вернуть.
    else:
        await send_telegram("Неизвестная команда. Введите /help для списка команд.")
        
    return {"ok": True}

@app.get("/", summary="Статус сервиса")
def read_root():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}
