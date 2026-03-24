import logging
import sqlite3
import json
import uuid
import ssl
from datetime import datetime
from typing import Optional

import certifi
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import SSLError, RequestException
from urllib3.util.retry import Retry

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, BotCommand
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WDM = True
except ImportError:
    HAS_WDM = False

try:
    from curl_cffi import requests as cf_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    print("⚠️ curl_cffi не установлен! pip install curl_cffi")

# ─────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────
TELEGRAM_BOT_TOKEN = "8277535481:AAFA9ihu7rSaszbos8G65sAd2rBG1Pp1dd0"
WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjYwMzAydjEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzkwMDQ5NDkxLCJpZCI6IjAxOWQxYjZiLTAzN2QtN2RlYS1hYmZkLWZkZGUyZjRjY2Y1NCIsImlpZCI6MjU1NjAzMjMsIm9pZCI6NTQwMDg5LCJzIjoxMDQwLCJzaWQiOiI4ZDcyOGI2Yi04MGZiLTRhYzctOTdjZS05YWVmNmNhZjg0YmUiLCJ0IjpmYWxzZSwidWlkIjoyNTU2MDMyM30.Ib_moeMpzG0AWtbEIQXjv1f8be_xnbq8m_Ar8TESOlhg0vn7AGTA6YdjYdW76P-yjRDCsAD2dKYDGG81hopBOQ"

POLLING_INTERVAL_NORMAL     = 60
POLLING_INTERVAL_AGGRESSIVE = 5

# Прокси для запросов к WB (если бот за рубежом — нужен российский прокси)
# Для Telegram VPN нужен, но WB может блокировать иностранные IP!
# Примеры:
# PROXY_URL = "socks5://user:pass@host:port"
# PROXY_URL = "http://user:pass@host:port"
PROXY_URL = None

# ─────────────────────────────────────────
# WB API URLs
# ─────────────────────────────────────────
WB_COMMON_API   = "https://common-api.wildberries.ru"
WB_SUPPLIES_API = "https://supplies-api.wildberries.ru"

COEF_URL       = f"{WB_COMMON_API}/api/tariffs/v1/acceptance/coefficients"
WAREHOUSES_URL = f"{WB_SUPPLIES_API}/api/v1/warehouses"
OPTIONS_URL    = f"{WB_SUPPLIES_API}/api/v1/acceptance/options"
SUPPLIES_URL   = f"{WB_SUPPLIES_API}/api/v1/supplies"

# ─────────────────────────────────────────
# STATES
# ─────────────────────────────────────────
STATE_API_KEY        = "api_key"
STATE_BARCODES       = "task_barcodes"
STATE_WAREHOUSE      = "task_warehouse"
STATE_ADD_BARCODE    = "add_barcode_to"
STATE_PHONE          = "wb_phone"
STATE_SMS_CODE       = "wb_sms_code"
STATE_SUPPLY_ID      = "wb_supply_id"

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    filename="wb_bot.log"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id INTEGER PRIMARY KEY,
            wb_api_key TEXT,
            aggressive_mode INTEGER DEFAULT 0,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS watch_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            task_type TEXT,
            barcodes TEXT,
            target_warehouse_id INTEGER,
            target_warehouse_name TEXT,
            source_warehouse_id INTEGER,
            source_warehouse_name TEXT,
            max_coefficient INTEGER DEFAULT 1,
            quantity INTEGER DEFAULT 1,
            active INTEGER DEFAULT 1,
            created_at TEXT,
            last_triggered TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS booking_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            task_id INTEGER,
            warehouse_name TEXT,
            coefficient REAL,
            date TEXT,
            status TEXT,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS wb_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER NOT NULL,
            phone TEXT NOT NULL,
            device_id TEXT NOT NULL,
            access_token TEXT,
            cookies TEXT,
            seller_token TEXT,
            is_active INTEGER DEFAULT 1,
            last_check TEXT,
            created_at TEXT,
            UNIQUE(tg_id, phone)
        )
    """)
    try:
        c.execute("ALTER TABLE watch_tasks ADD COLUMN supply_id TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE watch_tasks ADD COLUMN wb_account_phone TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE watch_tasks ADD COLUMN auto_book INTEGER DEFAULT 0")
    except Exception:
        pass
    conn.commit()
    conn.close()


def get_user(tg_id: int) -> Optional[dict]:
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("SELECT tg_id, wb_api_key, aggressive_mode FROM users WHERE tg_id=?", (tg_id,))
    row = c.fetchone()
    conn.close()
    return {"tg_id": row[0], "wb_api_key": row[1], "aggressive_mode": row[2]} if row else None


def save_user(tg_id: int, api_key: str):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO users (tg_id, wb_api_key, aggressive_mode, created_at)
        VALUES (?, ?, COALESCE((SELECT aggressive_mode FROM users WHERE tg_id=?), 0), ?)
    """, (tg_id, api_key, tg_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def get_tasks(tg_id: int, active_only=True) -> list:
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    q = "SELECT * FROM watch_tasks WHERE tg_id=?"
    if active_only:
        q += " AND active=1"
    c.execute(q, [tg_id])
    rows = c.fetchall()
    conn.close()
    tasks = []
    for r in rows:
        tasks.append({
            "id": r[0], "tg_id": r[1], "task_type": r[2],
            "barcodes": json.loads(r[3]) if r[3] else [],
            "target_warehouse_id": r[4], "target_warehouse_name": r[5],
            "source_warehouse_id": r[6], "source_warehouse_name": r[7],
            "max_coefficient": r[8], "quantity": r[9], "active": r[10],
            "created_at": r[11], "last_triggered": r[12],
            "supply_id": r[13] if len(r) > 13 else None,
            "wb_account_phone": r[14] if len(r) > 14 else None,
            "auto_book": r[15] if len(r) > 15 else 0,
        })
    return tasks


def add_task(tg_id, task_type, barcodes, target_id, target_name,
             source_id=None, source_name=None, max_coef=1, quantity=1) -> int:
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO watch_tasks
            (tg_id, task_type, barcodes, target_warehouse_id, target_warehouse_name,
             source_warehouse_id, source_warehouse_name, max_coefficient, quantity, active, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,1,?)
    """, (tg_id, task_type, json.dumps(barcodes), target_id, target_name,
          source_id, source_name, max_coef, quantity, datetime.now().isoformat()))
    task_id = c.lastrowid
    conn.commit()
    conn.close()
    return task_id


def deactivate_task(task_id: int):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("UPDATE watch_tasks SET active=0 WHERE id=?", (task_id,))
    conn.commit()
    conn.close()


def add_barcode_to_task(task_id: int, tg_id: int, barcode: str) -> tuple:
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("SELECT barcodes FROM watch_tasks WHERE id=? AND tg_id=?", (task_id, tg_id))
    row = c.fetchone()
    if not row:
        conn.close()
        return False, 0
    barcodes = json.loads(row[0]) if row[0] else []
    if barcode not in barcodes:
        barcodes.append(barcode)
    c.execute("UPDATE watch_tasks SET barcodes=? WHERE id=?", (json.dumps(barcodes), task_id))
    conn.commit()
    conn.close()
    return True, len(barcodes)


def log_booking(tg_id, task_id, wh_name, coef, date, status):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO booking_log (tg_id, task_id, warehouse_name, coefficient, date, status, created_at)
        VALUES (?,?,?,?,?,?,?)
    """, (tg_id, task_id, wh_name, coef, date, status, datetime.now().isoformat()))
    conn.commit()
    conn.close()


# ─── WB ACCOUNTS DB ─────────────────────
def save_wb_account(tg_id, phone, device_id, access_token=None, cookies=None, seller_token=None):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO wb_accounts (tg_id, phone, device_id, access_token, cookies,
                                 seller_token, is_active, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(tg_id, phone) DO UPDATE SET
            access_token=excluded.access_token,
            cookies=excluded.cookies,
            seller_token=excluded.seller_token,
            is_active=1,
            last_check=excluded.created_at
    """, (tg_id, phone, device_id, access_token,
          json.dumps(cookies or {}), seller_token, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def get_wb_accounts(tg_id):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        SELECT id, phone, access_token, cookies, seller_token, is_active, last_check, device_id
        FROM wb_accounts WHERE tg_id=? ORDER BY created_at DESC
    """, (tg_id,))
    rows = c.fetchall()
    conn.close()
    return [
        {"id": r[0], "phone": r[1], "access_token": r[2],
         "cookies": json.loads(r[3]) if r[3] else {},
         "seller_token": r[4], "is_active": r[5], "last_check": r[6], "device_id": r[7]}
        for r in rows
    ]


def get_wb_account_by_phone(tg_id, phone):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        SELECT id, phone, access_token, cookies, seller_token, is_active, last_check, device_id
        FROM wb_accounts WHERE tg_id=? AND phone=?
    """, (tg_id, phone))
    r = c.fetchone()
    conn.close()
    if not r:
        return None
    return {"id": r[0], "phone": r[1], "access_token": r[2],
            "cookies": json.loads(r[3]) if r[3] else {},
            "seller_token": r[4], "is_active": r[5], "last_check": r[6], "device_id": r[7]}


def delete_wb_account(tg_id, phone):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("DELETE FROM wb_accounts WHERE tg_id=? AND phone=?", (tg_id, phone))
    conn.commit()
    conn.close()


def update_account_tokens(tg_id, phone, access_token=None, cookies=None, seller_token=None):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        UPDATE wb_accounts SET access_token=?, cookies=?, seller_token=?, last_check=?
        WHERE tg_id=? AND phone=?
    """, (access_token, json.dumps(cookies or {}), seller_token,
          datetime.now().isoformat(), tg_id, phone))
    conn.commit()
    conn.close()


def mark_account_invalid(tg_id, phone):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("UPDATE wb_accounts SET is_active=0 WHERE tg_id=? AND phone=?", (tg_id, phone))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────
# WB API CLIENT (для коэффициентов — API-ключ)
# ─────────────────────────────────────────
class WBClient:
    def __init__(self, api_key: str):
        self.headers = {"Authorization": api_key, "Content-Type": "application/json"}

    def get_warehouses(self) -> list:
        try:
            r = requests.get(WAREHOUSES_URL, headers=self.headers, timeout=15)
            logger.info("get_warehouses status=%s len=%s", r.status_code, len(r.text))
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                return data
            # Иногда API возвращает dict с ошибкой
            logger.error("get_warehouses unexpected: %s", str(data)[:200])
            return []
        except Exception as e:
            logger.error("get_warehouses error: %s", e)
            return []

    def get_coefficients(self, warehouse_ids=None) -> list:
        try:
            params = {}
            if warehouse_ids:
                params["warehouseIDs"] = ",".join(map(str, warehouse_ids))
            r = requests.get(COEF_URL, headers=self.headers, params=params, timeout=15)
            logger.info("get_coefficients status=%s len=%s", r.status_code, len(r.text))
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                return data
            logger.error("get_coefficients unexpected: %s", str(data)[:200])
            return []
        except Exception as e:
            logger.error("get_coefficients error: %s", e)
            return []

    def get_acceptance_options(self, warehouse_id: int, barcodes: list) -> list:
        """Получает доступные даты/слоты для конкретных баркодов."""
        try:
            payload = {
                "warehouseID": warehouse_id,
                "barcodes": barcodes,
            }
            r = requests.post(OPTIONS_URL, headers=self.headers,
                              json=payload, timeout=15)
            logger.info("get_acceptance_options wh=%s status=%s body=%s",
                        warehouse_id, r.status_code, r.text[:300])
            if r.ok:
                return r.json() if isinstance(r.json(), list) else []
            return []
        except Exception as e:
            logger.error("get_acceptance_options: %s", e)
            return []

    def find_warehouse_by_name(self, name_part: str):
        for wh in self.get_warehouses():
            if name_part.lower() in wh.get("name", "").lower():
                return wh
        return None

    def book_supply_slot(self, supply_id, warehouse_id, slot_date, account=None):
        """Бронирует дату поставки. Использует curl_cffi если есть."""
        try:
            headers = {
                "Content-Type": "application/json",
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/124.0.0.0 Safari/537.36"),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "ru-RU,ru;q=0.9",
                "Origin": "https://seller.wildberries.ru",
                "Referer": "https://seller.wildberries.ru/",
            }
            cookies = {}
            if account:
                if account.get("access_token"):
                    headers["Authorization"] = account["access_token"]
                cookies = account.get("cookies", {})
            else:
                headers["Authorization"] = self.headers.get("Authorization", "")

            payload = {
                "supplyId": supply_id,
                "warehouseId": warehouse_id,
                "date": f"{slot_date}T00:00:00.000Z",
            }
            url = ("https://seller-supply.wildberries.ru/ns/sm-supply/"
                   "supply-manager/api/v1/supply/booking")
            proxies = {"https": PROXY_URL, "http": PROXY_URL} if PROXY_URL else None

            if HAS_CURL_CFFI:
                r = cf_requests.post(
                    url, json=payload, headers=headers,
                    cookies=cookies, impersonate="chrome124",
                    timeout=20, proxies=proxies
                )
            else:
                r = requests.post(
                    url, json=payload, headers=headers,
                    cookies=cookies, timeout=15, proxies=proxies
                )

            logger.info("book_supply_slot supply=%s wh=%s date=%s status=%s body=%s",
                        supply_id, warehouse_id, slot_date, r.status_code, r.text[:300])

            if r.status_code in (200, 201):
                return True, "Поставка забронирована ✅"
            data = r.json() if r.text else {}
            err = data.get("errorText") or data.get("detail") or r.text[:200]
            return False, f"Ошибка: {err}"
        except Exception as e:
            logger.error(f"book_supply_slot: {e}")
            return False, str(e)

# Активные Selenium-сессии по tg_id
_auth_drivers: dict = {}

class WBAuth:
    """Авторизация WB через Selenium — реальный браузер."""

    AUTH_URL = (
        "https://seller-auth.wildberries.ru/ru/"
        "?redirect_url=https%3A%2F%2Fseller.wildberries.ru%2F"
    )

    @staticmethod
    def new_device_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def _create_driver():
        opts = ChromeOptions()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        try:
            if HAS_WDM:
                svc = ChromeService(ChromeDriverManager().install())
            else:
                svc = ChromeService()
            driver = webdriver.Chrome(service=svc, options=opts)
        except Exception as e:
            raise RuntimeError(
                f"Chrome не запустился: {e}\n"
                "Установите: sudo apt install chromium-browser\n"
                "и: pip install webdriver-manager"
            )

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = {runtime: {}};
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ru-RU', 'ru']
                });
            """
        })
        return driver

    @staticmethod
    def _human_type(element, text, delay=0.07):
        for ch in text:
            element.send_keys(ch)
            time.sleep(delay)

    @staticmethod
    def _set_react_value(driver, element, value):
        driver.execute_script("""
            var el = arguments[0], val = arguments[1];
            var setter = Object.getOwnPropertyDescriptor(
                HTMLInputElement.prototype, 'value').set;
            setter.call(el, val);
            el.dispatchEvent(new Event('input',  {bubbles:true}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
        """, element, value)

    # ─── Шаг 1: ввести телефон ─────────────────────────
    @classmethod
    def request_sms(cls, phone: str, device_id: str, tg_id: int = 0) -> tuple:
        driver = None
        try:
            old = _auth_drivers.pop(tg_id, None)
            if old:
                try:
                    old.quit()
                except Exception:
                    pass

            driver = cls._create_driver()
            logger.info("WBAuth: открываю страницу авторизации…")
            driver.get(cls.AUTH_URL)
            time.sleep(4)

            try:
                driver.save_screenshot("wb_step1_loaded.png")
            except Exception:
                pass

            wait = WebDriverWait(driver, 25)

            # ── Ищем поле телефона ──
            phone_input = None
            for sel in [
                'input[data-testid="phone-input"]',
                'input[inputmode="numeric"]',
                'input[placeholder*="999"]',
                'input[type="tel"]',
            ]:
                try:
                    el = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
                    if el.is_displayed():
                        phone_input = el
                        logger.info("WBAuth: поле телефона: %s", sel)
                        break
                except TimeoutException:
                    continue

            if not phone_input:
                try:
                    driver.save_screenshot("wb_no_phone.png")
                except Exception:
                    pass
                driver.quit()
                return False, "Поле телефона не найдено (скриншот: wb_no_phone.png)"

            # 10 цифр без +7
            digits = phone.replace("+", "").replace(" ", "").replace("-", "")
            if digits.startswith("7") and len(digits) == 11:
                digits = digits[1:]
            if len(digits) != 10:
                driver.quit()
                return False, f"Нужно 10 цифр, получено {len(digits)}"

            phone_input.click()
            time.sleep(0.5)
            cls._human_type(phone_input, digits, delay=0.1)
            time.sleep(1)

            logger.info("WBAuth: телефон введён")

            # ── Ищем и кликаем чекбокс «Принимаю условия» ──
            checkbox_clicked = False
            for sel in [
                'input[type="checkbox"]',
                '[data-testid*="checkbox"]',
                '[data-testid*="terms"]',
                '[data-testid*="agree"]',
                '.Checkbox input',
                '.checkbox input',
            ]:
                try:
                    cb = driver.find_element(By.CSS_SELECTOR, sel)
                    if not cb.is_selected():
                        cb.click()
                        checkbox_clicked = True
                        logger.info("WBAuth: чекбокс кликнут: %s", sel)
                        time.sleep(0.5)
                    else:
                        checkbox_clicked = True
                        logger.info("WBAuth: чекбокс уже отмечен: %s", sel)
                    break
                except (NoSuchElementException, Exception):
                    continue

            # Если input не кликается — ищем label/span рядом
            if not checkbox_clicked:
                for sel in [
                    'label[class*="heckbox"]',
                    'span[class*="heckbox"]',
                    'div[class*="heckbox"]',
                    'label[class*="agree"]',
                    'label[data-testid*="terms"]',
                ]:
                    try:
                        el = driver.find_element(By.CSS_SELECTOR, sel)
                        if el.is_displayed():
                            el.click()
                            checkbox_clicked = True
                            logger.info("WBAuth: чекбокс-label кликнут: %s", sel)
                            time.sleep(0.5)
                            break
                    except Exception:
                        continue

            # Последняя попытка — по тексту
            if not checkbox_clicked:
                for el in driver.find_elements(By.XPATH, "//*"):
                    try:
                        t = el.text.lower()
                        tag = el.tag_name
                        if tag in ("label", "span", "div") and el.is_displayed():
                            if any(w in t for w in [
                                "принимаю", "соглас", "условия",
                                "условий", "accept", "terms", "agree"
                            ]):
                                el.click()
                                checkbox_clicked = True
                                logger.info("WBAuth: чекбокс по тексту: '%s'", t[:50])
                                time.sleep(0.5)
                                break
                    except Exception:
                        continue

            if not checkbox_clicked:
                logger.warning("WBAuth: чекбокс не найден — попробую без него")

            try:
                driver.save_screenshot("wb_step2_phone.png")
            except Exception:
                pass

            # ── Ищем и нажимаем кнопку ──
            submitted = False
            time.sleep(0.5)

            for sel in [
                'button[data-testid="submit-phone-btn"]',
                'button[data-testid="login-action-btn"]',
                'button[data-testid="submit-btn"]',
                'button[type="submit"]',
            ]:
                try:
                    btn = driver.find_element(By.CSS_SELECTOR, sel)
                    if btn.is_displayed():
                        logger.info("WBAuth: кнопка: %s text='%s' enabled=%s",
                                    sel, btn.text, btn.is_enabled())
                        if btn.is_enabled():
                            btn.click()
                            submitted = True
                            break
                        else:
                            # Попытка кликнуть через JS
                            driver.execute_script("arguments[0].click();", btn)
                            submitted = True
                            logger.info("WBAuth: кнопка кликнута через JS")
                            break
                except NoSuchElementException:
                    continue

            if not submitted:
                keywords = ["получить код", "далее", "войти", "отправить",
                            "продолжить", "запросить", "код"]
                for btn in driver.find_elements(By.TAG_NAME, "button"):
                    try:
                        t = btn.text.strip().lower()
                        if btn.is_displayed() and any(w in t for w in keywords):
                            logger.info("WBAuth: кнопка по тексту: '%s'", btn.text)
                            if btn.is_enabled():
                                btn.click()
                            else:
                                driver.execute_script("arguments[0].click();", btn)
                            submitted = True
                            break
                    except Exception:
                        continue

            if not submitted:
                # Любая видимая кнопка
                for btn in driver.find_elements(By.TAG_NAME, "button"):
                    try:
                        if btn.is_displayed():
                            logger.info("WBAuth: fallback кнопка: '%s'", btn.text)
                            driver.execute_script("arguments[0].click();", btn)
                            submitted = True
                            break
                    except Exception:
                        continue

            if not submitted:
                logger.info("WBAuth: кнопка не найдена, Enter")
                phone_input.send_keys(Keys.ENTER)

            time.sleep(4)

            try:
                driver.save_screenshot("wb_step3_after_click.png")
            except Exception:
                pass

            # ── Проверяем появление OTP-полей ──
            code_appeared = False

            # 6 OTP-полей
            try:
                WebDriverWait(driver, 15).until(
                    lambda d: len(d.find_elements(
                        By.CSS_SELECTOR, 'input[data-testid="sms-code-input"]'
                    )) >= 4
                )
                code_appeared = True
                logger.info("WBAuth: OTP-поля появились!")
            except TimeoutException:
                pass

            if not code_appeared:
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((
                            By.CSS_SELECTOR,
                            'input[autocomplete="one-time-code"]'
                        ))
                    )
                    code_appeared = True
                    logger.info("WBAuth: поле one-time-code появилось")
                except TimeoutException:
                    pass

            if not code_appeared:
                try:
                    body = driver.find_element(By.TAG_NAME, "body").text
                    if "введите код" in body.lower() or "код из смс" in body.lower():
                        code_appeared = True
                        logger.info("WBAuth: текст 'введите код' найден")
                except Exception:
                    pass

            if not code_appeared:
                try:
                    driver.save_screenshot("wb_step4_no_code.png")
                except Exception:
                    pass
                try:
                    body = driver.find_element(By.TAG_NAME, "body").text
                    if any(w in body.lower() for w in [
                        "ошибк", "error", "заблокир", "лимит",
                        "слишком часто", "попробуйте позже"
                    ]):
                        driver.quit()
                        return False, f"Ошибка WB: {body[:300]}"
                except Exception:
                    pass
                logger.warning("WBAuth: OTP не появился")
            # ── Внедряем перехватчик сетевых ответов ──
            try:
                driver.execute_script("""
                    window._authResponses = [];

                    // Перехват fetch
                    const origFetch = window.fetch;
                    window.fetch = async function(...args) {
                        const resp = await origFetch.apply(this, args);
                        const clone = resp.clone();
                        try {
                            const text = await clone.text();
                            window._authResponses.push({
                                url: typeof args[0] === 'string' ? args[0] : args[0].url,
                                status: resp.status,
                                body: text
                            });
                        } catch(e) {}
                        return resp;
                    };

                    // Перехват XMLHttpRequest
                    const origOpen = XMLHttpRequest.prototype.open;
                    const origSend = XMLHttpRequest.prototype.send;
                    XMLHttpRequest.prototype.open = function(method, url) {
                        this._captureUrl = url;
                        this._captureMethod = method;
                        return origOpen.apply(this, arguments);
                    };
                    XMLHttpRequest.prototype.send = function() {
                        this.addEventListener('load', function() {
                            try {
                                window._authResponses.push({
                                    url: this._captureUrl,
                                    status: this.status,
                                    body: this.responseText
                                });
                            } catch(e) {}
                        });
                        return origSend.apply(this, arguments);
                    };

                    console.log('Auth interceptor installed');
                """)
                logger.info("WBAuth: сетевой перехватчик установлен")
            except Exception as e:
                logger.warning("WBAuth: не удалось установить перехватчик: %s", e)
            _auth_drivers[tg_id] = driver

            if code_appeared:
                return True, "SMS отправлена ✅"
            else:
                return True, (
                    "Кнопка нажата, но OTP-поле не найдено.\n"
                    "SMS могла прийти — введите код если получили.\n"
                    "Скриншоты: wb_step3_after_click.png, wb_step4_no_code.png"
                )

        except TimeoutException:
            if driver:
                try:
                    driver.save_screenshot("wb_timeout.png")
                except Exception:
                    pass
                driver.quit()
            return False, "Страница WB не загрузилась (таймаут)"
        except Exception as e:
            logger.error("WBAuth.request_sms: %s", e)
            if driver:
                try:
                    driver.save_screenshot("wb_error.png")
                except Exception:
                    pass
                try:
                    driver.quit()
                except Exception:
                    pass
            return False, f"Ошибка: {e}"

    # ─── Шаг 2: ввести код из SMS ─────────────────────
    @classmethod
    def confirm_code(cls, phone: str, code: str,
                     device_id: str, tg_id: int = 0) -> tuple:
        driver = _auth_drivers.get(tg_id)
        if not driver:
            return False, {
                "error": "Браузер не найден. Начните заново через /accounts"
            }

        try:
            code = str(code).strip()
            time.sleep(1)

            try:
                driver.save_screenshot("wb_code_before.png")
            except Exception:
                pass

            # ── Ищем 6 OTP-полей ──
            otp_inputs = driver.find_elements(
                By.CSS_SELECTOR, 'input[data-testid="sms-code-input"]'
            )
            logger.info("WBAuth: OTP-полей: %d, код: '%s' (%d цифр)",
                        len(otp_inputs), code, len(code))

            if otp_inputs and len(otp_inputs) >= len(code):
                for i, ch in enumerate(code):
                    otp_inputs[i].click()
                    time.sleep(0.15)
                    otp_inputs[i].send_keys(ch)
                    time.sleep(0.2)
                logger.info("WBAuth: код введён в %d OTP-полей", len(code))
            else:
                # Одно поле или fallback
                code_input = None
                for sel in [
                    'input[autocomplete="one-time-code"]',
                    'input[data-testid="code-input"]',
                    'input[inputmode="numeric"]:not([data-testid="phone-input"])',
                ]:
                    try:
                        el = driver.find_element(By.CSS_SELECTOR, sel)
                        if el.is_displayed():
                            code_input = el
                            break
                    except NoSuchElementException:
                        continue

                if code_input:
                    code_input.click()
                    time.sleep(0.2)
                    cls._human_type(code_input, code, delay=0.1)
                else:
                    driver.quit()
                    _auth_drivers.pop(tg_id, None)
                    return False, {"error": "Поле для кода не найдено"}

            # ── Ждём обработку ──
            time.sleep(3)

            try:
                driver.save_screenshot("wb_code_after.png")
            except Exception:
                pass

            # ── Проверяем ошибку ──
            try:
                body = driver.find_element(By.TAG_NAME, "body").text.lower()
                if any(w in body for w in ["неверный", "неправильный",
                                           "invalid", "ошибка кода"]):
                    return False, {"error": "Неверный код. Попробуйте ещё раз."}
            except Exception:
                pass

            # ── Кнопка подтверждения ──
            for sel in [
                'button[data-testid="confirm-code-btn"]',
                'button[type="submit"]',
            ]:
                try:
                    btn = driver.find_element(By.CSS_SELECTOR, sel)
                    if btn.is_displayed() and btn.is_enabled():
                        btn.click()
                        time.sleep(2)
                        break
                except Exception:
                    continue

            # ── ГЛАВНОЕ: Читаем перехваченные сетевые ответы ──
            time.sleep(5)

            access_token = ""
            cookies = {}
            auth_data = {}

            try:
                responses = driver.execute_script(
                    "return window._authResponses || [];"
                )
                logger.info("WBAuth: перехвачено %d сетевых ответов", len(responses))

                for resp in responses:
                    url = resp.get("url", "")
                    status = resp.get("status", 0)
                    body_str = resp.get("body", "")

                    logger.info("WBAuth: ответ url=%s status=%s body=%s",
                                url[:80], status, body_str[:200])

                    # Ищем ответ авторизации
                    if status == 200 and body_str:
                        try:
                            data = json.loads(body_str)

                            # Ищем токен в разных форматах ответа
                            token = (
                                    data.get("token")
                                    or data.get("access_token")
                                    or data.get("accessToken")
                                    or (data.get("payload", {}) or {}).get("token", "")
                                    or (data.get("payload", {}) or {}).get("access_token", "")
                                    or (data.get("result", {}) or {}).get("token", "")
                            )

                            if token and len(str(token)) > 20:
                                access_token = str(token)
                                auth_data = data
                                logger.info(
                                    "WBAuth: ТОКЕН НАЙДЕН! len=%d url=%s",
                                    len(access_token), url[:80]
                                )
                                break

                        except (json.JSONDecodeError, AttributeError):
                            pass

            except Exception as e:
                logger.warning("WBAuth: ошибка чтения перехвата: %s", e)

            # ── Собираем cookies с текущей страницы ──
            for c in driver.get_cookies():
                cookies[c["name"]] = c["value"]
                if not access_token and c["name"] in (
                        "WBTokenV3", "WBToken", "x-auth-token",
                        "wbx-seller-token", "wb-auth-token"
                ):
                    access_token = c["value"]

            # ── Пробуем перейти на seller (может не загрузиться через VPN) ──
            try:
                driver.get("https://seller.wildberries.ru/")
                time.sleep(4)
                for c in driver.get_cookies():
                    cookies[c["name"]] = c["value"]
                    if not access_token and c["name"] in (
                            "WBTokenV3", "WBToken", "x-auth-token",
                            "wbx-seller-token", "wb-auth-token"
                    ):
                        access_token = c["value"]
            except Exception:
                pass

            # ── Ищем токен в localStorage ──
            if not access_token:
                try:
                    token_js = driver.execute_script("""
                        return localStorage.getItem('token')
                            || localStorage.getItem('access_token')
                            || localStorage.getItem('WBToken')
                            || localStorage.getItem('WBTokenV3')
                            || sessionStorage.getItem('token')
                            || sessionStorage.getItem('access_token')
                            || '';
                    """)
                    if token_js and len(token_js) > 20:
                        access_token = token_js
                        logger.info("WBAuth: токен из storage len=%d", len(token_js))
                except Exception:
                    pass

            final_url = driver.current_url

            # ── Если токен есть — устанавливаем его как cookie ──
            if access_token:
                cookies["WBTokenV3"] = access_token

            logger.info(
                "WBAuth: итог url=%s token=%d cookies=%d wb_keys=%s",
                final_url, len(access_token), len(cookies),
                [k for k in cookies if "token" in k.lower() or "wb" in k.lower()]
            )

            try:
                driver.save_screenshot("wb_final.png")
            except Exception:
                pass

            driver.quit()
            _auth_drivers.pop(tg_id, None)

            if access_token:
                return True, {
                    "access_token": access_token,
                    "cookies": cookies,
                    "raw": auth_data or {"url": final_url},
                }
            elif len(cookies) > 5:
                return True, {
                    "access_token": "",
                    "cookies": cookies,
                    "raw": {"url": final_url, "note": "token not found, using cookies only"},
                }
            else:
                return False, {
                    "error": (
                        f"Токен не получен.\n"
                        f"URL: {final_url}\n"
                        f"Cookies: {list(cookies.keys())}\n"
                        "Возможно VPN блокирует WB."
                    )
                }

        except Exception as e:
            logger.error("WBAuth.confirm_code: %s", e)
            try:
                driver.save_screenshot("wb_code_error.png")
            except Exception:
                pass
            try:
                driver.quit()
            except Exception:
                pass
            _auth_drivers.pop(tg_id, None)
            return False, {"error": f"Ошибка: {e}"}

    # ─── Проверка сессии ───────────────────────────────
    @classmethod
    def check_session(cls, account: dict) -> tuple:
        if not account:
            return False, "нет данных"

        cookies = account.get("cookies", {})
        access_token = account.get("access_token", "")

        if not access_token and not cookies:
            return False, "нет токена и cookies"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://seller.wildberries.ru",
            "Referer": "https://seller.wildberries.ru/",
        }

        # Устанавливаем токен в разных форматах
        if access_token:
            headers["Authorization"] = access_token
            # Также добавляем в cookies
            cookies["WBTokenV3"] = access_token

        # Пробуем несколько endpoint-ов
        tests = [
            ("POST",
             "https://seller-supply.wildberries.ru/ns/sm-supply/"
             "supply-manager/api/v1/supply/list",
             {"filter": {"statusIDs": [1, 2]}, "limit": 1, "offset": 0}),
            ("GET",
             "https://seller-content.wildberries.ru/ns/characteristics-configurator-api/"
             "content-configurator/api/v1/config/get/object/all?top=10&lang=ru",
             None),
        ]

        for method, url, payload in tests:
            try:
                if method == "POST":
                    r = requests.post(url, json=payload, headers=headers,
                                      cookies=cookies, timeout=15)
                else:
                    r = requests.get(url, headers=headers,
                                     cookies=cookies, timeout=15)

                logger.info("check_session %s status=%s body=%s",
                            url.split("/")[-2], r.status_code, r.text[:150])

                if r.status_code == 200:
                    return True, "активна ✅"
                if r.status_code in (401, 403):
                    continue
            except Exception as e:
                logger.error("check_session: %s", e)
                continue

        return False, "сессия не работает (401) — возможно VPN блокирует"

    # ─── Очистка драйвера ──────────────────────────────
    @classmethod
    def cleanup_driver(cls, tg_id: int):
        d = _auth_drivers.pop(tg_id, None)
        if d:
            try:
                d.quit()
            except Exception:
                pass

# ─────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────
def main_menu_keyboard():
    return ReplyKeyboardMarkup([
        ["👤 Аккаунты WB", "📊 Коэффициенты Сарапул"],
        ["🏪 Все склады", "➕ Создать задачу"],
        ["📋 Мои задачи", "⚡ Агрессивный режим"],
        ["📜 История броней", "⚙️ Настройки"],
    ], resize_keyboard=True)


def cancel_keyboard():
    return ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)


def coef_emoji(coef) -> str:
    return {0: "🟢", 1: "🟡", -1: "🔴"}.get(coef, "🟠")


def format_coefficients(coefs, max_items=20):
    if not coefs:
        return "Нет данных"
    lines, seen = [], set()
    for c in coefs:
        wh = c.get("warehouseName", "?")
        date = c.get("date", "")[:10]
        key = f"{wh}_{date}"
        if key in seen:
            continue
        seen.add(key)
        coef = c.get("coefficient", -1)
        allow = c.get("allowUnload", False)
        avail = " ✅" if (coef in (0, 1) and allow) else ""
        lines.append(f"{coef_emoji(coef)} {wh} | {date} | коэф: {coef}{avail}")
        if len(lines) >= max_items:
            break
    return "\n".join(lines)


# ─────────────────────────────────────────
# MONITORING ENGINE
# ─────────────────────────────────────────
async def run_monitoring_cycle(app):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("SELECT DISTINCT tg_id FROM watch_tasks WHERE active=1")
    user_ids = [row[0] for row in c.fetchall()]
    conn.close()

    for tg_id in user_ids:
        user = get_user(tg_id)
        if not user or not user["wb_api_key"]:
            continue
        tasks = get_tasks(tg_id, active_only=True)
        if not tasks:
            continue
        client = WBClient(user["wb_api_key"])
        target_ids = list({t["target_warehouse_id"] for t in tasks if t["target_warehouse_id"]})
        coefs = client.get_coefficients(target_ids or None)
        for task in tasks:
            try:
                await check_task(app, tg_id, task, coefs)
            except Exception as e:
                logger.error(f"check_task task={task['id']}: {e}")


async def check_task(app, tg_id, task, coefs):
    target_id = task["target_warehouse_id"]
    max_coef = task["max_coefficient"]
    matches = [
        c for c in coefs
        if c.get("warehouseID") == target_id
        and 0 <= c.get("coefficient", -1) <= max_coef
        and c.get("allowUnload", False) is True
    ]
    if not matches:
        return

    best = min(matches, key=lambda x: (x["coefficient"], x["date"]))
    coef_val = best["coefficient"]
    slot_date = best.get("date", "")[:10]
    wh_name = best.get("warehouseName", task["target_warehouse_name"])
    barcodes_str = (
        ", ".join(task["barcodes"][:3]) + ("..." if len(task["barcodes"]) > 3 else "")
        if task["barcodes"] else "⚠️ баркоды не добавлены"
    )

    auto_book = task.get("auto_book", 0)
    supply_id = task.get("supply_id")
    account_phone = task.get("wb_account_phone")

    booked_ok = False
    booking_status = ""

    if auto_book and supply_id and account_phone:
        account = get_wb_account_by_phone(tg_id, account_phone)
        if account and account["is_active"]:
            valid, sess_status = WBAuth.check_session(account)
            if valid:
                user = get_user(tg_id)
                client = WBClient(user["wb_api_key"] if user else WB_API_KEY)
                booked_ok, booking_status = client.book_supply_slot(
                    supply_id=supply_id, warehouse_id=target_id,
                    slot_date=slot_date, account=account
                )
                if booked_ok:
                    deactivate_task(task["id"])
            else:
                mark_account_invalid(tg_id, account_phone)
                booking_status = f"⚠️ Сессия {account_phone} истекла — войдите заново"
        else:
            booking_status = f"⚠️ Аккаунт {account_phone} недоступен"

    if booked_ok:
        msg = (
            f"🎉 <b>ПОСТАВКА ЗАБРОНИРОВАНА АВТОМАТИЧЕСКИ!</b>\n\n"
            f"🏪 Склад: <b>{wh_name}</b>\n"
            f"📅 Дата: <b>{slot_date}</b>\n"
            f"📊 Коэффициент: <b>{coef_val}</b>\n"
            f"📦 Поставка: <b>{supply_id}</b>\n\n"
            f"{booking_status}\n\n"
            f"✅ Задача #{task['id']} закрыта.\n"
            f"🔗 <a href='https://seller.wildberries.ru/supplies-management/"
            f"all-supplies'>Проверить</a>"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📋 Мои задачи", callback_data="my_tasks"),
        ]])
    else:
        warn = f"\n⚠️ {booking_status}" if booking_status else ""
        auto_hint = (
            "\n\n💡 Для автобронирования — добавьте аккаунт WB (/accounts)"
            if not auto_book else ""
        )
        msg = (
            f"🚨 <b>СЛОТ ДОСТУПЕН!</b>\n\n"
            f"🏪 Склад: <b>{wh_name}</b>\n"
            f"📅 Дата: <b>{slot_date}</b>\n"
            f"📊 Коэффициент: <b>{coef_val}</b>\n"
            f"✅ Разгрузка: разрешена\n\n"
            f"📦 Задача #{task['id']}\n"
            f"Баркоды: {barcodes_str}{warn}{auto_hint}\n\n"
            f"⚡ Действуйте немедленно!\n"
            f"🔗 <a href='https://seller.wildberries.ru/supplies-management/"
            f"all-supplies'>Открыть кабинет WB</a>"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Зафиксировано", callback_data=f"ack_{task['id']}"),
            InlineKeyboardButton("🛑 Остановить", callback_data=f"stop_{task['id']}"),
        ]])

    try:
        await app.bot.send_message(
            chat_id=tg_id, text=msg, parse_mode="HTML",
            reply_markup=keyboard, disable_web_page_preview=True
        )
        log_booking(tg_id, task["id"], wh_name, coef_val, slot_date,
                    "AUTO_BOOKED" if booked_ok else "NOTIFIED")
    except Exception as e:
        logger.error(f"send_message: {e}")


# ─────────────────────────────────────────
# КОМАНДЫ
# ─────────────────────────────────────────
BOT_COMMANDS = [
    BotCommand("start",      "🏠 Главное меню"),
    BotCommand("accounts",   "👤 Аккаунты WB"),
    BotCommand("status",     "📊 Статус"),
    BotCommand("checkall",   "🔍 Проверить сейчас"),
    BotCommand("tasks",      "📋 Задачи"),
    BotCommand("newtask",    "➕ Создать задачу"),
    BotCommand("testaccount", "🧪 Тестовый запрос к WB"),
    BotCommand("addbarcode", "📦 Добавить баркод"),
    BotCommand("aggressive", "⚡ Агрессивный режим"),
    BotCommand("history",    "📜 История"),
    BotCommand("setkey",     "🔑 API ключ"),
    BotCommand("help",       "📖 Справка"),
]


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    if not get_user(tg_id):
        save_user(tg_id, WB_API_KEY)
    ctx.user_data.clear()

    curl_status = "✅ curl_cffi" if HAS_CURL_CFFI else "❌ curl_cffi НЕ установлен!"
    await update.message.reply_text(
        f"🤖 <b>WB Авто-бронирование</b>\n\n"
        f"TLS-движок: {curl_status}\n\n"
        "Мониторю коэффициенты и мгновенно уведомляю / бронирую.\n"
        "🎯 Приоритетный склад: <b>Сарапул</b>\n\n"
        "Выберите действие:",
        parse_mode="HTML", reply_markup=main_menu_keyboard()
    )


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    tasks = get_tasks(tg_id)
    mode = "⚡ 5 сек" if user and user["aggressive_mode"] else "💤 60 сек"
    curl = "✅" if HAS_CURL_CFFI else "❌"
    proxy = f"✅ {PROXY_URL[:30]}..." if PROXY_URL else "❌ нет"
    await update.message.reply_text(
        f"📊 <b>Статус</b>\n\n"
        f"🔄 Режим: {mode}\n"
        f"📋 Задач: {len(tasks)}\n"
        f"🔐 curl_cffi: {curl}\n"
        f"🌐 Прокси: {proxy}\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        parse_mode="HTML"
    )


async def cmd_checkall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    if not user:
        await update.message.reply_text("⚠️ /setkey")
        return
    msg = await update.message.reply_text("🔍 Проверяю...")
    client = WBClient(user["wb_api_key"])
    coefs = client.get_coefficients()
    tasks = get_tasks(tg_id)
    triggered = 0
    for task in tasks:
        await check_task(ctx.application, tg_id, task, coefs)
        if any(
            c.get("warehouseID") == task["target_warehouse_id"]
            and c.get("coefficient", -1) in (0, 1) and c.get("allowUnload")
            for c in coefs
        ):
            triggered += 1
    await msg.edit_text(
        f"✅ Задач: {len(tasks)} | Сработало: {triggered} | {datetime.now().strftime('%H:%M:%S')}"
    )


async def cmd_tasks(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_tasks(update, ctx)


async def cmd_newtask(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await create_task_start(update, ctx)


async def cmd_addbarcode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("📦 /addbarcode <ID> <баркод>")
        return
    tg_id = update.effective_user.id
    try:
        task_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ ID — число")
        return
    barcode = args[1].strip()
    ok, total = add_barcode_to_task(task_id, tg_id, barcode)
    if not ok:
        await update.message.reply_text(f"❌ Задача #{task_id} не найдена")
        return
    await update.message.reply_text(
        f"✅ <code>{barcode}</code> → #{task_id} (всего: {total})",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"➕ Ещё", callback_data=f"addb_{task_id}"),
            InlineKeyboardButton("📋 Задачи", callback_data="my_tasks"),
        ]])
    )


async def cmd_aggressive(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await toggle_aggressive(update, ctx)


async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_booking_history(update, ctx)


async def cmd_setkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["waiting_for"] = STATE_API_KEY
    await update.message.reply_text("🔑 Введите WB API ключ:", reply_markup=cancel_keyboard())


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 <b>Справка</b>\n\n"
        "/start — меню\n"
        "/accounts — аккаунты WB (вход через SMS)\n"
        "/newtask — создать задачу\n"
        "/tasks — задачи\n"
        "/checkall — проверить сейчас\n"
        "/aggressive — 5 сек режим\n"
        "/addbarcode &lt;id&gt; &lt;баркод&gt;\n"
        "/history — история\n"
        "/setkey — API ключ\n\n"
        "🔐 <b>Важно:</b> для авторизации в WB нужен <code>curl_cffi</code>:\n"
        "<code>pip install curl_cffi</code>",
        parse_mode="HTML"
    )


async def cmd_accounts(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_accounts_menu(update, ctx)

async def cmd_testaccount(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Полный тест аккаунта WB."""
    tg_id = update.effective_user.id
    accounts = get_wb_accounts(tg_id)

    if not accounts:
        await update.message.reply_text("❌ Нет аккаунтов. /accounts")
        return

    acc = accounts[0]
    phone = acc["phone"]
    msg = await update.message.reply_text(f"🧪 Тестирую {phone}...")

    cookies = acc.get("cookies", {})
    access_token = acc.get("access_token", "")

    results = []
    results.append(f"🔑 Token: {'✅ ' + str(len(access_token)) + ' символов' if access_token else '❌ пустой'}")

    wb_cookies = [k for k in cookies if "wb" in k.lower() or "token" in k.lower() or "auth" in k.lower()]
    results.append(f"🍪 Auth cookies: {wb_cookies or '❌ нет'}")

    headers = {
        "User-Agent": "Mozilla/5.0 Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://seller.wildberries.ru",
        "Referer": "https://seller.wildberries.ru/",
    }
    if access_token:
        headers["Authorization"] = access_token
        cookies["WBTokenV3"] = access_token

    # Тест 1: Поставки
    results.append("\n<b>— Тесты API —</b>")
    try:
        r = requests.post(
            "https://seller-supply.wildberries.ru/ns/sm-supply/"
            "supply-manager/api/v1/supply/list",
            json={"filter": {"statusIDs": [1, 2]}, "limit": 5, "offset": 0},
            headers=headers, cookies=cookies, timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            supplies = data.get("supplies", data.get("result", []))
            if isinstance(supplies, list):
                results.append(f"✅ Поставки: {len(supplies)} шт.")
                for s in supplies[:3]:
                    sid = s.get("supplyId") or s.get("id") or s.get("name", "?")
                    results.append(f"   📦 {sid}")
            else:
                results.append(f"✅ Поставки: ответ получен")
        else:
            results.append(f"❌ Поставки: {r.status_code} {r.text[:100]}")
    except Exception as e:
        results.append(f"❌ Поставки: {e}")

    # Тест 2: Информация о продавце
    try:
        r = requests.get(
            "https://seller-content.wildberries.ru/ns/"
            "characteristics-configurator-api/content-configurator/"
            "api/v1/config/get/object/all?top=5&lang=ru",
            headers=headers, cookies=cookies, timeout=15,
        )
        if r.status_code == 200:
            results.append(f"✅ Content API: доступен")
        else:
            results.append(f"❌ Content API: {r.status_code}")
    except Exception as e:
        results.append(f"❌ Content API: {e}")

    # Тест 3: Склады (API ключ)
    results.append("\n<b>— API ключ —</b>")
    user = get_user(tg_id)
    if user and user["wb_api_key"]:
        try:
            r = requests.get(
                WAREHOUSES_URL,
                headers={"Authorization": user["wb_api_key"]},
                timeout=15,
            )
            if r.status_code == 200:
                whs = r.json()
                results.append(f"✅ Склады: {len(whs)} шт.")
                for w in whs[:3]:
                    results.append(f"   🏪 {w.get('name', '?')} (ID:{w.get('ID', '?')})")
            else:
                results.append(f"❌ Склады: {r.status_code} {r.text[:100]}")
                results.append("   ⚠️ VPN может блокировать WB API!")
        except Exception as e:
            results.append(f"❌ Склады: {e}")
            results.append("   ⚠️ Скорее всего VPN блокирует запрос к WB")
    else:
        results.append("❌ API ключ не задан (/setkey)")

    # Тест 4: Коэффициенты
    try:
        r = requests.get(COEF_URL,
                         headers={"Authorization": user["wb_api_key"] if user else ""},
                         timeout=15)
        if r.status_code == 200:
            coefs = r.json()
            results.append(f"✅ Коэффициенты: {len(coefs)} записей")
        else:
            results.append(f"❌ Коэффициенты: {r.status_code}")
    except Exception as e:
        results.append(f"❌ Коэффициенты: {e}")

    # Итог
    results.append("\n<b>— Диагноз —</b>")
    if not access_token:
        results.append(
            "⚠️ Токен пустой — авторизация не завершилась.\n"
            "Причина: VPN блокирует seller.wildberries.ru\n"
            "Решение: отключите VPN при добавлении аккаунта,\n"
            "или используйте прокси только для Telegram."
        )
    elif any("❌ Поставки" in r for r in results):
        results.append(
            "⚠️ Токен есть, но API не принимает.\n"
            "Возможно токен невалидный или VPN мешает."
        )
    else:
        results.append("✅ Всё работает!")

    text = (
        f"🧪 <b>Тест {phone}</b>\n\n"
        + "\n".join(results) + "\n\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}"
    )
    await msg.edit_text(text, parse_mode="HTML")

# ─────────────────────────────────────────
# ТЕКСТОВОЕ МЕНЮ
# ─────────────────────────────────────────
async def handle_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    waiting = ctx.user_data.get("waiting_for")
    if waiting:
        await handle_input(update, ctx, waiting)
        return

    text = update.message.text
    handlers_map = {
        "📊 Коэффициенты Сарапул": show_sarapul_coefs,
        "🏪 Все склады": show_all_warehouses,
        "➕ Создать задачу": create_task_start,
        "📋 Мои задачи": show_tasks,
        "⚡ Агрессивный режим": toggle_aggressive,
        "📜 История броней": show_booking_history,
        "⚙️ Настройки": show_settings,
        "👤 Аккаунты WB": show_accounts_menu,
    }
    handler = handlers_map.get(text)
    if handler:
        await handler(update, ctx)
    else:
        await update.message.reply_text("Выберите из меню или /help",
                                        reply_markup=main_menu_keyboard())


# ─────────────────────────────────────────
# ДИАЛОГОВЫЙ ВВОД (без дубликатов!)
# ─────────────────────────────────────────
async def handle_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE, waiting: str):
    text = update.message.text
    tg_id = update.effective_user.id

    if text == "❌ Отмена":
        WBAuth.cleanup_driver(tg_id)
        ctx.user_data.clear()
        await update.message.reply_text("Отменено.", reply_markup=main_menu_keyboard())
        return

    # ── API ключ ──
    if waiting == STATE_API_KEY:
        api_key = text.strip()
        if len(api_key) < 50:
            await update.message.reply_text("⚠️ Слишком короткий. Попробуйте ещё:")
            return
        save_user(tg_id, api_key)
        ctx.user_data.clear()
        await update.message.reply_text("✅ Сохранён!", reply_markup=main_menu_keyboard())

    # ── Баркоды ──
    elif waiting == STATE_BARCODES:
        if text.strip() == "0":
            barcodes = []
        else:
            raw = text.replace("\n", ",").replace(";", ",")
            barcodes = [b.strip() for b in raw.split(",") if b.strip()]
            if not barcodes:
                await update.message.reply_text("⚠️ Введите через запятую:")
                return

        if "creating_task" not in ctx.user_data:
            ctx.user_data["creating_task"] = {"type": "booking"}
        ctx.user_data["creating_task"]["barcodes"] = barcodes
        ctx.user_data["waiting_for"] = "task_quantity"
        await update.message.reply_text(
            f"✅ Баркодов: {len(barcodes)}\n\n"
            "📦 Введите количество коробов (штук):\n"
            "Например: 4",
            reply_markup=cancel_keyboard()
        )

    # ── Количество ──
    elif waiting == "task_quantity":
        try:
            qty = int(text.strip())
            if qty < 1:
                raise ValueError
        except ValueError:
            await update.message.reply_text("⚠️ Введите число больше 0:")
            return

        ctx.user_data["creating_task"]["quantity"] = qty
        ctx.user_data["waiting_for"] = STATE_WAREHOUSE
        await update.message.reply_text(
            f"✅ Количество: {qty}\n\n🏪 Введите название склада:",
            reply_markup=cancel_keyboard()
        )

    # ── Склад ──
    elif waiting == STATE_WAREHOUSE:
        wh_input = text.strip()
        user = get_user(tg_id)
        client = WBClient(user["wb_api_key"] if user else WB_API_KEY)
        msg = await update.message.reply_text("🔍 Ищу...")
        wh = client.find_warehouse_by_name(wh_input)
        if not wh:
            await msg.edit_text(f"❌ «{wh_input}» не найден. Попробуйте другое:")
            return

        task_data = ctx.user_data.get("creating_task", {})
        barcodes = task_data.get("barcodes", [])
        qty = task_data.get("quantity", 1)
        task_id = add_task(
            tg_id=tg_id, task_type=task_data.get("type", "booking"),
            barcodes=barcodes, target_id=wh["ID"], target_name=wh["name"],
            max_coef=1, quantity=qty
        )
        ctx.user_data.clear()

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Баркод", callback_data=f"addb_{task_id}"),
            InlineKeyboardButton("📋 Задачи", callback_data="my_tasks"),
        ]])
        await msg.edit_text(
            f"✅ <b>Задача #{task_id}</b>\n"
            f"🏪 {wh['name']} | 📦 {len(barcodes)} баркодов | 📊 {qty} коробов",
            parse_mode="HTML", reply_markup=kb
        )
        await update.message.reply_text("OK", reply_markup=main_menu_keyboard())

    # ── Номер телефона ──
    elif waiting == STATE_PHONE:
        phone = text.strip()
        if phone.startswith("8") and len(phone) == 11 and phone.isdigit():
            phone = "+7" + phone[1:]
        elif not phone.startswith("+"):
            phone = "+" + phone

        if len(phone) != 12 or not phone[1:].isdigit():
            await update.message.reply_text(
                "❌ Формат: +7XXXXXXXXXX", reply_markup=cancel_keyboard()
            )
            return

        device_id = WBAuth.new_device_id()
        ctx.user_data["auth_phone"] = phone
        ctx.user_data["auth_device_id"] = device_id

        msg = await update.message.reply_text(f"📱 Отправляю SMS на {phone}...")
        ok, err_text = WBAuth.request_sms(phone, device_id, tg_id=tg_id)
        if not ok:
            await msg.edit_text(
                f"❌ Ошибка: {err_text}\n\n"
                f"{'⚠️ curl_cffi не установлен! pip install curl_cffi' if not HAS_CURL_CFFI else ''}\n"
                "Проверьте номер / сеть и попробуйте снова:"
            )
            # Остаёмся в STATE_PHONE — пользователь может ввести номер снова
            return

        ctx.user_data["waiting_for"] = STATE_SMS_CODE
        await msg.edit_text(f"✅ SMS → {phone}\n\n📩 Введите код:")

    # ── SMS-код ──
    elif waiting == STATE_SMS_CODE:
        code = text.strip()
        phone = ctx.user_data.get("auth_phone", "")
        device_id = ctx.user_data.get("auth_device_id", "")

        if not phone or not device_id:
            ctx.user_data.clear()
            await update.message.reply_text("Сессия истекла. /accounts",
                                            reply_markup=main_menu_keyboard())
            return

        msg = await update.message.reply_text("🔐 Проверяю...")
        ok, session = WBAuth.confirm_code(phone, code, device_id, tg_id=tg_id)
        if not ok:
            err = session.get("error", "неверный код")
            try:
                await msg.edit_text(f"❌ {err}")
            except Exception:
                pass
            await update.message.reply_text(
                "Введите код ещё раз или нажмите Отмена:",
                reply_markup=cancel_keyboard()
            )
            ctx.user_data["waiting_for"] = STATE_SMS_CODE
            return

        save_wb_account(
            tg_id=tg_id, phone=phone, device_id=device_id,
            access_token=session.get("access_token"),
            cookies=session.get("cookies", {}),
        )
        ctx.user_data.clear()
        await msg.edit_text(
            f"✅ <b>Аккаунт {phone} добавлен!</b>\n\n"
            "Бот может бронировать от вашего имени.\n"
            "При создании задачи укажите аккаунт и ID поставки.",
            parse_mode="HTML"
        )
        await update.message.reply_text("OK", reply_markup=main_menu_keyboard())

    # ── ID поставки ──
    elif waiting == STATE_SUPPLY_ID:
        supply_id = text.strip()
        task_id = ctx.user_data.get("supply_task_id")
        account_phone = ctx.user_data.get("supply_account_phone")
        if not task_id:
            ctx.user_data.clear()
            await update.message.reply_text("Истекло.", reply_markup=main_menu_keyboard())
            return
        conn = sqlite3.connect("wb_bot.db")
        c = conn.cursor()
        c.execute(
            "UPDATE watch_tasks SET supply_id=?, wb_account_phone=?, auto_book=1 "
            "WHERE id=? AND tg_id=?",
            (supply_id, account_phone, task_id, tg_id)
        )
        conn.commit()
        conn.close()
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ <b>Автобронирование:</b>\n📦 {supply_id}\n👤 {account_phone}",
            parse_mode="HTML", reply_markup=main_menu_keyboard()
        )

    # ── Добавление баркода ──
    elif waiting == STATE_ADD_BARCODE:
        task_id = ctx.user_data.get("add_barcode_task_id")
        if not task_id:
            ctx.user_data.clear()
            await update.message.reply_text("Истекло.", reply_markup=main_menu_keyboard())
            return
        barcode = text.strip()
        ok, total = add_barcode_to_task(task_id, tg_id, barcode)
        if not ok:
            await update.message.reply_text(f"❌ Задача #{task_id} не найдена")
            ctx.user_data.clear()
            return
        await update.message.reply_text(
            f"✅ <code>{barcode}</code> → #{task_id} ({total} шт.)\n\nЕщё или Готово:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("➕ Ещё", callback_data=f"addb_{task_id}"),
                InlineKeyboardButton("✅ Готово", callback_data="my_tasks"),
            ]])
        )


# ─────────────────────────────────────────
# ЭКРАНЫ
# ─────────────────────────────────────────
async def show_sarapul_coefs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    if not user:
        await update.message.reply_text("⚠️ /start")
        return
    msg = await update.message.reply_text("⏳ Загружаю...")
    client = WBClient(user["wb_api_key"])
    whs = client.get_warehouses()
    if not whs:
        await msg.edit_text(
            "❌ <b>Не удалось получить склады от WB API</b>\n\n"
            "• Проверьте VPN (WB API может блокироваться)\n"
            "• Проверьте API-ключ: /setkey",
            parse_mode="HTML"
        )
        return
    wh = None
    for w in whs:
        if "сарапул" in w.get("name", "").lower():
            wh = w
            break
    if not wh:
        names = ", ".join(w.get("name", "?") for w in whs[:10])
        await msg.edit_text(
            f"❌ Сарапул не найден среди {len(whs)} складов.\n\n"
            f"Доступные: {names}{'...' if len(whs) > 10 else ''}"
        )
        return
    coefs = client.get_coefficients([wh["ID"]])
    if not coefs:
        await msg.edit_text(f"❌ Нет данных (ID: {wh['ID']})")
        return
    available = [c for c in coefs if c.get("coefficient", -1) in (0, 1) and c.get("allowUnload")]
    text = f"🏪 <b>Сарапул (ID: {wh['ID']})</b>\n🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
    if available:
        text += "✅ <b>ДОСТУПНО:</b>\n"
        for c in available[:5]:
            text += f"  {coef_emoji(c['coefficient'])} {c.get('date','')[:10]} | {c['coefficient']}\n"
        text += "\n"
    text += "<b>Все даты:</b>\n"
    seen = set()
    for c in coefs:
        key = c.get("date", "")[:10]
        if key in seen:
            continue
        seen.add(key)
        coef = c.get("coefficient", -1)
        allow = c.get("allowUnload", False)
        s = "✅" if (coef in (0, 1) and allow) else ("🔴" if coef == -1 else "🟠")
        text += f"  {s} {key} | {coef} | box:{c.get('boxTypeID','')}\n"
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_sarapul"),
        InlineKeyboardButton("🔔 Следить", callback_data=f"watch_{wh['ID']}_{wh['name']}"),
    ]])
    await msg.edit_text(text, parse_mode="HTML", reply_markup=kb)


async def show_all_warehouses(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    if not user:
        return
    msg = await update.message.reply_text("⏳ Загружаю склады...")
    client = WBClient(user["wb_api_key"])
    coefs = client.get_coefficients()
    if not coefs:
        await msg.edit_text(
            "❌ <b>WB API не отвечает</b>\n\n"
            "Причины:\n"
            "• 🌐 VPN блокирует WB API (самая частая)\n"
            "• 🔑 API-ключ истёк\n\n"
            "<b>Решение:</b>\n"
            "1. Отключите VPN\n"
            "2. Или настройте split-tunnel (VPN только для Telegram)\n"
            "3. Проверьте ключ: /setkey\n"
            "4. Диагностика: /testaccount",
            parse_mode="HTML"
        )
        return
    best = {}
    for c in coefs:
        name = c.get("warehouseName", "?")
        coef = c.get("coefficient", -1)
        allow = c.get("allowUnload", False)
        if name not in best or (coef >= 0 and coef < best[name]["coef"]):
            best[name] = {"coef": coef, "allow": allow, "date": c.get("date", "")[:10]}
    avail = sorted(
        [(n, i) for n, i in best.items() if i["coef"] in (0, 1) and i["allow"]],
        key=lambda x: x[1]["coef"]
    )
    unavail = sorted(
        [(n, i) for n, i in best.items() if not (i["coef"] in (0, 1) and i["allow"])],
        key=lambda x: x[0]
    )
    text = f"🏪 <b>Склады</b> | {datetime.now().strftime('%H:%M')}\n\n"
    if avail:
        text += f"✅ <b>ДОСТУПНЫ ({len(avail)}):</b>\n"
        for n, i in avail[:15]:
            text += f"  🟢 {n} | {i['coef']} | {i['date']}\n"
        text += "\n"
    text += f"❌ <b>Нет ({len(unavail)}):</b>\n"
    for n, i in unavail[:20]:
        text += f"  {'🟠' if i['coef'] > 1 else '🔴'} {n} | {i['coef']}\n"
    if len(unavail) > 20:
        text += f"  ...+{len(unavail)-20}\n"
    await msg.edit_text(text, parse_mode="HTML")


async def create_task_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Бронирование", callback_data="task_booking")],
        [InlineKeyboardButton("🔄 Перераспределение", callback_data="task_redist")],
        [InlineKeyboardButton("⚡ Быстро: Сарапул", callback_data="task_sarapul_quick")],
    ])
    await update.message.reply_text("📝 <b>Тип задачи:</b>", parse_mode="HTML", reply_markup=kb)


async def show_tasks(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    tasks = get_tasks(tg_id)
    if not tasks:
        await update.message.reply_text("📋 Нет задач.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("➕ Создать", callback_data="new_task")
            ]]))
        return
    text = f"📋 <b>Задачи ({len(tasks)}):</b>\n\n"
    buttons = []
    for t in tasks:
        bc = f"{len(t['barcodes'])}" if t["barcodes"] else "⚠️0"
        auto = "🤖" if t.get("auto_book") else ""
        text += (
            f"<b>#{t['id']}</b> {t['target_warehouse_name']} | "
            f"📦{bc} | коэф≤{t['max_coefficient']} {auto}\n"
        )
        buttons.append([
            InlineKeyboardButton("➕📦", callback_data=f"addb_{t['id']}"),
            InlineKeyboardButton("🤖Авто", callback_data=f"setup_autobook_{t['id']}"),
            InlineKeyboardButton("🛑", callback_data=f"stop_{t['id']}"),
        ])
    await update.message.reply_text(text, parse_mode="HTML",
                                    reply_markup=InlineKeyboardMarkup(buttons))


async def toggle_aggressive(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    if not user:
        return
    new_mode = 1 - user["aggressive_mode"]
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("UPDATE users SET aggressive_mode=? WHERE tg_id=?", (new_mode, tg_id))
    conn.commit()
    conn.close()

    scheduler = ctx.application.bot_data.get("scheduler")
    if scheduler:
        interval = POLLING_INTERVAL_AGGRESSIVE if new_mode else POLLING_INTERVAL_NORMAL
        job = scheduler.get_job("monitoring")
        if job:
            job.reschedule("interval", seconds=interval)

    text = "⚡ <b>Агрессивный (5 сек) ВКЛ</b>" if new_mode else "💤 <b>Стандартный (60 сек)</b>"
    await update.message.reply_text(text, parse_mode="HTML")


async def show_booking_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute(
        "SELECT warehouse_name, coefficient, date, status, created_at "
        "FROM booking_log WHERE tg_id=? ORDER BY id DESC LIMIT 20",
        (tg_id,)
    )
    rows = c.fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("📜 Пусто.")
        return
    text = "📜 <b>История:</b>\n\n"
    for r in rows:
        text += f"  🏪 {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4][:16]}\n"
    await update.message.reply_text(text, parse_mode="HTML")


async def show_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    mode = "⚡ 5с" if user and user["aggressive_mode"] else "💤 60с"
    curl = "✅" if HAS_CURL_CFFI else "❌"
    proxy = "✅" if PROXY_URL else "❌"
    await update.message.reply_text(
        f"⚙️ <b>Настройки</b>\n\n"
        f"🔑 API: {'✅' if user else '❌'}\n"
        f"⏱ Режим: {mode}\n"
        f"🔐 curl_cffi: {curl}\n"
        f"🌐 Прокси: {proxy}\n\n"
        "/setkey — сменить ключ",
        parse_mode="HTML"
    )


async def show_accounts_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    accounts = get_wb_accounts(tg_id)

    if not accounts:
        text = (
            "👤 <b>Аккаунты WB</b>\n\n"
            "Нет привязанных аккаунтов.\n\n"
            f"{'✅ curl_cffi установлен' if HAS_CURL_CFFI else '❌ curl_cffi НЕ установлен — pip install curl_cffi'}\n\n"
            "Войдите через телефон для автобронирования:"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Войти в WB", callback_data="acc_add"),
        ]])
    else:
        text = f"👤 <b>Аккаунты ({len(accounts)})</b>\n\n"
        buttons = []
        for acc in accounts:
            s = "🟢" if acc["is_active"] else "🔴"
            text += f"{s} <code>{acc['phone']}</code>\n"
            buttons.append([
                InlineKeyboardButton(f"🔍 {acc['phone']}", callback_data=f"acc_check_{acc['phone']}"),
                InlineKeyboardButton("🗑", callback_data=f"acc_del_{acc['phone']}"),
            ])
        buttons.append([InlineKeyboardButton("➕ Добавить", callback_data="acc_add")])
        kb = InlineKeyboardMarkup(buttons)

    if update.message:
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await update.callback_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)


# ─────────────────────────────────────────
# CALLBACK HANDLER
# ─────────────────────────────────────────
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    tg_id = query.from_user.id
    user = get_user(tg_id)
    client = WBClient(user["wb_api_key"] if user else WB_API_KEY)

    if data == "refresh_sarapul":
        wh = client.find_warehouse_by_name("Сарапул")
        if not wh:
            await query.edit_message_text("❌ Не найден")
            return
        coefs = client.get_coefficients([wh["ID"]])
        avail = [c for c in coefs if c.get("coefficient", -1) in (0, 1) and c.get("allowUnload")]
        s = f"✅ {len(avail)} слотов" if avail else "❌ Нет"
        text = (
            f"🏪 <b>Сарапул</b> | {datetime.now().strftime('%H:%M:%S')}\n{s}\n\n"
            f"{format_coefficients(coefs, 14)}"
        )
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=query.message.reply_markup)

    elif data.startswith("watch_"):
        parts = data.split("_", 2)
        wh_id, wh_name = int(parts[1]), parts[2] if len(parts) > 2 else "Склад"
        task_id = add_task(tg_id=tg_id, task_type="booking", barcodes=[],
                          target_id=wh_id, target_name=wh_name)
        await query.edit_message_text(
            f"✅ #{task_id} → {wh_name}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("➕📦", callback_data=f"addb_{task_id}")
            ]])
        )

    elif data.startswith("addb_"):
        task_id = int(data.split("_")[1])
        ctx.user_data["waiting_for"] = STATE_ADD_BARCODE
        ctx.user_data["add_barcode_task_id"] = task_id
        await query.message.reply_text(f"📦 Баркод для #{task_id}:", reply_markup=cancel_keyboard())

    elif data == "task_sarapul_quick":
        wh = client.find_warehouse_by_name("Сарапул")
        if not wh:
            await query.edit_message_text("❌ Не найден")
            return
        task_id = add_task(tg_id=tg_id, task_type="booking", barcodes=[],
                          target_id=wh["ID"], target_name=wh["name"])
        ctx.user_data["waiting_for"] = STATE_ADD_BARCODE
        ctx.user_data["add_barcode_task_id"] = task_id
        await query.edit_message_text(f"✅ #{task_id} Сарапул\n📦 Введите баркод:", parse_mode="HTML")
        await query.message.reply_text("Баркод:", reply_markup=cancel_keyboard())

    elif data in ("task_booking", "task_redist"):
        task_type = "booking" if data == "task_booking" else "redistribution"
        ctx.user_data["creating_task"] = {"type": task_type}
        ctx.user_data["waiting_for"] = STATE_BARCODES
        await query.edit_message_text(
            "Введите баркоды через запятую (или 0 чтобы пропустить):",
            parse_mode="HTML"
        )

    elif data in ("my_tasks", "new_task"):
        ctx.user_data.clear()
        await query.message.reply_text("Используйте меню:", reply_markup=main_menu_keyboard())

    elif data.startswith("stop_"):
        task_id = int(data.split("_")[1])
        deactivate_task(task_id)
        await query.edit_message_text(
            (query.message.text or "") + f"\n\n🛑 #{task_id} остановлена", parse_mode="HTML"
        )

    elif data.startswith("ack_"):
        await query.edit_message_text(
            (query.message.text or "") + "\n\n✅ OK", parse_mode="HTML"
        )

    elif data == "acc_add":
        ctx.user_data["waiting_for"] = STATE_PHONE
        await query.message.reply_text(
            "📱 Введите номер телефона WB аккаунта:\n"
            "Формат: +7XXXXXXXXXX",
            reply_markup=cancel_keyboard()
        )

    elif data.startswith("acc_check_"):
        phone = data[len("acc_check_"):]
        account = get_wb_account_by_phone(tg_id, phone)
        if not account:
            await query.edit_message_text("❌ Не найден")
            return
        msg = await query.message.reply_text(f"🔍 Проверяю {phone}...")
        valid, status = WBAuth.check_session(account)
        if valid:
            update_account_tokens(tg_id, phone,
                                  access_token=account["access_token"],
                                  cookies=account["cookies"])
            await msg.edit_text(f"✅ {phone}: активна")
        else:
            mark_account_invalid(tg_id, phone)
            await msg.edit_text(
                f"🔴 {phone}: {status}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Войти заново", callback_data="acc_add")
                ]])
            )

    elif data.startswith("acc_del_"):
        phone = data[len("acc_del_"):]
        delete_wb_account(tg_id, phone)
        await query.answer(f"{phone} удалён")
        # Обновляем список
        await show_accounts_menu(update, ctx)

    elif data.startswith("setup_autobook_"):
        task_id = int(data.split("_")[2])
        accounts = get_wb_accounts(tg_id)
        if not accounts:
            await query.edit_message_text("⚠️ Нет аккаунтов. /accounts")
            return
        buttons = [[
            InlineKeyboardButton(
                f"{'🟢' if a['is_active'] else '🔴'} {a['phone']}",
                callback_data=f"autobook_acc_{task_id}_{a['phone']}"
            )
        ] for a in accounts]
        await query.edit_message_text(
            f"🤖 Аккаунт для #{task_id}:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif data.startswith("autobook_acc_"):
        # autobook_acc_<task_id>_<phone>
        rest = data[len("autobook_acc_"):]
        idx = rest.index("_")
        task_id = int(rest[:idx])
        account_phone = rest[idx+1:]

        account = get_wb_account_by_phone(tg_id, account_phone)
        if not account:
            await query.edit_message_text("❌ Аккаунт не найден")
            return

        ctx.user_data["waiting_for"] = STATE_SUPPLY_ID
        ctx.user_data["supply_task_id"] = task_id
        ctx.user_data["supply_account_phone"] = account_phone

        await query.edit_message_text(
            f"✅ Аккаунт: {account_phone}\n\n"
            "📦 Введите ID поставки (из кабинета WB):\n"
            "Поставка должна быть создана БЕЗ даты."
        )
        await query.message.reply_text("ID поставки:", reply_markup=cancel_keyboard())


# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────
async def post_init(app):
    await app.bot.set_my_commands(BOT_COMMANDS)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_monitoring_cycle, "interval",
        seconds=POLLING_INTERVAL_NORMAL, id="monitoring",
        args=[app], misfire_grace_time=30,
    )
    scheduler.start()
    app.bot_data["scheduler"] = scheduler
    logger.info("Bot started, interval=%s, curl_cffi=%s, proxy=%s",
                POLLING_INTERVAL_NORMAL, HAS_CURL_CFFI, bool(PROXY_URL))


def main():
    init_db()
    # Если нужен прокси ТОЛЬКО для Telegram (а WB идёт напрямую):
    # from telegram.request import HTTPXRequest
    # request = HTTPXRequest(proxy="socks5://127.0.0.1:1080")  # ваш VPN/прокси
    # .request(request)

    builder = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init)

    # Если Telegram заблокирован — используем прокси ТОЛЬКО для Telegram.
    # WB API и авторизация пойдут напрямую (без VPN).
    # Укажите адрес вашего VPN/прокси:
    TELEGRAM_PROXY = None  # "socks5://206.123.156.200:7572" или "http://127.0.0.1:8080"

    if TELEGRAM_PROXY:
        from telegram.request import HTTPXRequest
        request = HTTPXRequest(proxy=TELEGRAM_PROXY)
        builder = builder.request(request)
        print(f"📡 Telegram через прокси: {TELEGRAM_PROXY}")
    else:
        print("📡 Telegram напрямую (убедитесь что VPN включён системно)")

    app = builder.build()

    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("accounts",   cmd_accounts))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(CommandHandler("checkall",   cmd_checkall))
    app.add_handler(CommandHandler("tasks",      cmd_tasks))
    app.add_handler(CommandHandler("newtask",    cmd_newtask))
    app.add_handler(CommandHandler("testaccount", cmd_testaccount))
    app.add_handler(CommandHandler("addbarcode", cmd_addbarcode))
    app.add_handler(CommandHandler("aggressive", cmd_aggressive))
    app.add_handler(CommandHandler("history",    cmd_history))
    app.add_handler(CommandHandler("setkey",     cmd_setkey))
    app.add_handler(CommandHandler("help",       cmd_help))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu))

    print(f"🤖 WB Bot запущен! curl_cffi={'✅' if HAS_CURL_CFFI else '❌'}")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
