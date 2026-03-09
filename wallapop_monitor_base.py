import os
import re
import time
import uuid
import sqlite3
import logging
import threading
import unicodedata
from pathlib import Path
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from flask import Flask, request, redirect, url_for, render_template_string, flash, jsonify, session


# ============================================================
# CONFIG
# ============================================================
def load_local_env() -> None:
    base_dir = Path(__file__).resolve().parent
    candidates = [
        base_dir / ".env",
        Path.cwd() / ".env",
        Path.cwd() / "´scrapper" / ".env",
    ]

    loaded_paths = set()
    for env_path in candidates:
        resolved = str(env_path.resolve())
        if resolved in loaded_paths or not env_path.exists():
            continue
        loaded_paths.add(resolved)

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def env_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


load_local_env()
APP_SECRET = env_str("APP_SECRET", "cambia-esta-clave")
APP_PASSWORD = env_str("APP_PASSWORD", "cambia-esta-password")
DB_PATH = env_str("DB_PATH", "wallapop_app.db")
POLL_SECONDS = int(env_str("POLL_SECONDS", "300"))  # 5 minutos
TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN", "PON_AQUI_TU_BOT_TOKEN")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID", "PON_AQUI_TU_CHAT_ID")
WALLAPOP_APP_VERSION = env_str("WALLAPOP_APP_VERSION", "8.1737.0")
WALLAPOP_APP_VERSION_HEADER = env_str("WALLAPOP_APP_VERSION_HEADER", WALLAPOP_APP_VERSION.replace(".", ""))
FILTER_TODAY_ONLY = env_bool("FILTER_TODAY_ONLY", True)
PORT = int(env_str("PORT", "8080"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

app = Flask(__name__)
app.secret_key = APP_SECRET
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = False
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# ============================================================
# DB
# ============================================================
@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS watch_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                keywords TEXT NOT NULL,
                min_price REAL,
                max_price REAL,
                title_must_include TEXT,
                title_must_include_or TEXT,
                title_must_not_include TEXT,
                description_must_include TEXT,
                description_must_include_or TEXT,
                must_not_include TEXT,
                match_mode TEXT NOT NULL DEFAULT 'smart',
                smart_min_score REAL NOT NULL DEFAULT 65,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute("PRAGMA table_info(watch_rules)")
        watch_rules_columns = {row[1] for row in cur.fetchall()}
        if "title_must_include_or" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN title_must_include_or TEXT")
        if "title_must_not_include" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN title_must_not_include TEXT")
        if "description_must_include_or" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN description_must_include_or TEXT")
        if "match_mode" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN match_mode TEXT NOT NULL DEFAULT 'smart'")
        if "smart_min_score" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN smart_min_score REAL NOT NULL DEFAULT 65")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_items (
                item_id TEXT NOT NULL,
                watch_id INTEGER NOT NULL,
                title TEXT,
                price REAL,
                url TEXT,
                seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (item_id, watch_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                watch_id INTEGER,
                item_id TEXT,
                title TEXT,
                price REAL,
                url TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def split_csv(text: Optional[str]) -> list[str]:
    if not text:
        return []
    return [x.strip() for x in text.split(",") if x.strip()]


# ============================================================
# AUTH
# ============================================================
def login_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("is_logged_in"):
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)
    return wrapped


# ============================================================
# UTILS
# ============================================================
def normalize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    # Remove accents to match variants like "edicion" vs "edición".
    normalized = unicodedata.normalize("NFKD", str(text))
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", normalized).strip().lower()


def get_rule_value(rule: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        value = rule[key]
    except Exception:
        return default
    if value is None:
        return default
    return value


def clamp_score(value: Any, default: float = 65.0) -> float:
    try:
        score = float(value)
    except Exception:
        score = default
    return max(0.0, min(100.0, score))


def compact_text(text: Optional[str]) -> str:
    normalized = normalize_text(text)
    return re.sub(r"[^a-z0-9]+", "", normalized)


def contains_term(text: Optional[str], term: Optional[str]) -> bool:
    text_n = normalize_text(text)
    term_n = normalize_text(term)
    if not term_n:
        return True
    if term_n in text_n:
        return True

    # Extra tolerance for variants like "1 tb" vs "1tb".
    return compact_text(term_n) in compact_text(text_n)


def extract_price(text: str) -> Optional[float]:
    text = normalize_text(text)
    match = re.search(r"(\d+[\.,]?\d*)\s*€", text)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def send_telegram(message: str) -> tuple[bool, str]:
    if (
        not TELEGRAM_BOT_TOKEN
        or not TELEGRAM_CHAT_ID
        or "PON_AQUI" in TELEGRAM_BOT_TOKEN
        or "PON_AQUI" in TELEGRAM_CHAT_ID
    ):
        return False, "Telegram no configurado"

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": False,
    }
    try:
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        if not data.get("ok", False):
            return False, data.get("description", "Telegram devolvió una respuesta no válida")
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def build_search_url(rule: sqlite3.Row) -> str:
    params = {
        "keywords": rule["keywords"],
        "source": "search_box",
        "order_by": "newest",
    }
    if FILTER_TODAY_ONLY:
        params["time_filter"] = "today"
    if rule["min_price"] is not None:
        params["min_sale_price"] = rule["min_price"]
    if rule["max_price"] is not None:
        params["max_sale_price"] = rule["max_price"]
    return f"https://es.wallapop.com/search?{urlencode(params)}"


def build_wallapop_api_headers(referer_url: str) -> dict[str, str]:
    device_id = str(uuid.uuid4())
    return {
        **HEADERS,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://es.wallapop.com",
        "Referer": referer_url,
        "x-deviceos": "0",
        "x-deviceid": device_id,
        "x-devicetoken": device_id,
        "x-appversion": WALLAPOP_APP_VERSION_HEADER,
        "x-semanticversion": WALLAPOP_APP_VERSION,
        "x-wallapop-viewport-breakpoint": "L",
    }


def extract_price_amount(item: dict[str, Any]) -> Optional[float]:
    price_data = item.get("price")
    if isinstance(price_data, dict):
        amount = price_data.get("amount")
        if isinstance(amount, (int, float)):
            return float(amount)
    if isinstance(price_data, (int, float)):
        return float(price_data)
    return None


def is_created_today_utc(created_at_ms: Any) -> bool:
    if created_at_ms is None:
        return False
    try:
        created_dt = datetime.fromtimestamp(float(created_at_ms) / 1000.0, tz=timezone.utc)
    except Exception:
        return False
    return created_dt.date() == datetime.now(timezone.utc).date()


def fetch_search_results_legacy_html(search_url: str) -> list[dict]:
    response = requests.get(search_url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    items = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/item/" not in href:
            continue
        full_url = href if href.startswith("http") else f"https://es.wallapop.com{href}"
        title = link.get_text(" ", strip=True)
        item_id_match = re.search(r"-(\d+)$", href)
        item_id = item_id_match.group(1) if item_id_match else full_url
        parent_text = link.parent.get_text(" ", strip=True) if link.parent else title
        price = extract_price(parent_text)
        items.append(
            {
                "item_id": item_id,
                "title": title,
                "description": "",
                "price": price,
                "url": full_url,
                "created_at": None,
            }
        )
    return items


def fetch_search_results(rule: sqlite3.Row) -> list[dict]:
    search_url = build_search_url(rule)
    params: dict[str, Any] = {
        "keywords": rule["keywords"],
        "source": "search_box",
        "order_by": "newest",
    }
    if FILTER_TODAY_ONLY:
        params["time_filter"] = "today"
    if rule["min_price"] is not None:
        params["min_sale_price"] = rule["min_price"]
    if rule["max_price"] is not None:
        params["max_sale_price"] = rule["max_price"]

    api_headers = build_wallapop_api_headers(search_url)
    try:
        response = requests.get("https://api.wallapop.com/api/v3/search", params=params, headers=api_headers, timeout=30)
        response.raise_for_status()
        payload = response.json()
        raw_items = (
            payload.get("data", {})
            .get("section", {})
            .get("payload", {})
            .get("items", [])
        )

        items: list[dict] = []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            created_at = raw.get("created_at")
            if FILTER_TODAY_ONLY and not is_created_today_utc(created_at):
                continue

            item_id = str(raw.get("id") or "").strip()
            web_slug = str(raw.get("web_slug") or "").strip()
            if not item_id:
                item_id = web_slug
            if not item_id:
                continue

            item_url = f"https://es.wallapop.com/item/{web_slug}" if web_slug else search_url
            items.append(
                {
                    "item_id": item_id,
                    "title": str(raw.get("title") or "").strip(),
                    "description": str(raw.get("description") or "").strip(),
                    "price": extract_price_amount(raw),
                    "url": item_url,
                    "created_at": created_at,
                }
            )

        logging.info("Wallapop API: %s resultados para %s", len(items), rule["name"])
        return items
    except Exception as exc:
        logging.exception("Fallo en API de Wallapop, usando fallback HTML: %s", exc)
        return fetch_search_results_legacy_html(search_url)


def fetch_item_description(item_url: str) -> str:
    response = requests.get(item_url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    return soup.get_text(" ", strip=True)[:5000]


def rule_match_details(rule: sqlite3.Row, title: str, description: str) -> tuple[bool, list[str]]:
    title_n = normalize_text(title)
    desc_n = normalize_text(description)
    whole = f"{title_n} {desc_n}"
    reasons: list[str] = []

    include_terms = split_csv(get_rule_value(rule, "title_must_include", ""))
    include_or_terms = split_csv(get_rule_value(rule, "title_must_include_or", ""))
    if include_terms or include_or_terms:
        include_ok = bool(include_terms) and all(contains_term(title_n, term) for term in include_terms)
        include_or_ok = bool(include_or_terms) and any(contains_term(title_n, term) for term in include_or_terms)
        if not (include_ok or include_or_ok):
            for term in include_terms:
                if not contains_term(title_n, term):
                    reasons.append(f"title:{term}")
            if include_or_terms:
                reasons.append(f"title_or:{'|'.join(include_or_terms)}")

    for term in split_csv(get_rule_value(rule, "title_must_not_include", "")):
        if contains_term(title_n, term):
            reasons.append(f"title_excluded:{term}")

    desc_include_terms = split_csv(get_rule_value(rule, "description_must_include", ""))
    desc_include_or_terms = split_csv(get_rule_value(rule, "description_must_include_or", ""))
    if desc_include_terms or desc_include_or_terms:
        desc_include_ok = bool(desc_include_terms) and all(contains_term(desc_n, term) for term in desc_include_terms)
        desc_include_or_ok = bool(desc_include_or_terms) and any(contains_term(desc_n, term) for term in desc_include_or_terms)
        if not (desc_include_ok or desc_include_or_ok):
            for term in desc_include_terms:
                if not contains_term(desc_n, term):
                    reasons.append(f"description:{term}")
            if desc_include_or_terms:
                reasons.append(f"description_or:{'|'.join(desc_include_or_terms)}")

    for term in split_csv(get_rule_value(rule, "must_not_include", "")):
        if contains_term(whole, term):
            reasons.append(f"excluded:{term}")

    return len(reasons) == 0, reasons


def rule_match_smart_details(rule: sqlite3.Row, title: str, description: str) -> tuple[bool, float, list[str]]:
    title_n = normalize_text(title)
    desc_n = normalize_text(description)
    whole = f"{title_n} {desc_n}"

    title_required_terms = split_csv(get_rule_value(rule, "title_must_include", ""))
    title_or_terms = split_csv(get_rule_value(rule, "title_must_include_or", ""))
    desc_required_terms = split_csv(get_rule_value(rule, "description_must_include", ""))
    desc_or_terms = split_csv(get_rule_value(rule, "description_must_include_or", ""))

    reasons: list[str] = []
    for term in split_csv(get_rule_value(rule, "title_must_not_include", "")):
        if contains_term(title_n, term):
            reasons.append(f"title_excluded:{term}")
    for term in split_csv(get_rule_value(rule, "must_not_include", "")):
        if contains_term(whole, term):
            reasons.append(f"excluded:{term}")
    if reasons:
        return False, 0.0, reasons

    title_required_hits = [contains_term(title_n, term) for term in title_required_terms]
    title_or_hit = any(contains_term(title_n, term) for term in title_or_terms) if title_or_terms else False
    desc_required_hits = [contains_term(desc_n, term) for term in desc_required_terms]
    desc_or_hit = any(contains_term(desc_n, term) for term in desc_or_terms) if desc_or_terms else False

    total_weight = 0.0
    matched_weight = 0.0
    if title_required_terms:
        total_weight += 22.0 * len(title_required_terms)
        matched_weight += 22.0 * sum(1 for hit in title_required_hits if hit)
    if title_or_terms:
        total_weight += 16.0
        if title_or_hit:
            matched_weight += 16.0
    if desc_required_terms:
        total_weight += 12.0 * len(desc_required_terms)
        matched_weight += 12.0 * sum(1 for hit in desc_required_hits if hit)
    if desc_or_terms:
        total_weight += 10.0
        if desc_or_hit:
            matched_weight += 10.0

    if total_weight == 0:
        return True, 100.0, []

    score = round((matched_weight / total_weight) * 100.0, 1)
    min_score = clamp_score(get_rule_value(rule, "smart_min_score", 65), 65.0)
    if score >= min_score:
        return True, score, []

    reasons.append(f"score:{score}<{min_score}")
    for term, hit in zip(title_required_terms, title_required_hits):
        if not hit:
            reasons.append(f"title:{term}")
    if title_or_terms and not title_or_hit:
        reasons.append(f"title_or:{'|'.join(title_or_terms)}")
    for term, hit in zip(desc_required_terms, desc_required_hits):
        if not hit:
            reasons.append(f"description:{term}")
    if desc_or_terms and not desc_or_hit:
        reasons.append(f"description_or:{'|'.join(desc_or_terms)}")

    return False, score, reasons


def evaluate_rule_match(rule: sqlite3.Row, title: str, description: str) -> tuple[bool, list[str], Optional[float], str]:
    mode = normalize_text(str(get_rule_value(rule, "match_mode", "smart")))
    if mode == "smart":
        ok, score, reasons = rule_match_smart_details(rule, title, description)
        return ok, reasons, score, "smart"

    ok, reasons = rule_match_details(rule, title, description)
    return ok, reasons, None, "strict"


def rule_matches(rule: sqlite3.Row, title: str, description: str) -> bool:
    ok, _, _, _ = evaluate_rule_match(rule, title, description)
    return ok


def already_seen(watch_id: int, item_id: str) -> bool:
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM seen_items WHERE watch_id = ? AND item_id = ?",
            (watch_id, item_id),
        )
        return cur.fetchone() is not None


def mark_seen(watch_id: int, item_id: str, title: str, price: Optional[float], url: str):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO seen_items (item_id, watch_id, title, price, url)
            VALUES (?, ?, ?, ?, ?)
            """,
            (item_id, watch_id, title, price, url),
        )


def log_notification(watch_id: int, item_id: str, title: str, price: Optional[float], url: str, status: str):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO notifications_log (watch_id, item_id, title, price, url, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (watch_id, item_id, title, price, url, status),
        )


# ============================================================
# MONITOR LOOP
# ============================================================
def process_rule(rule: sqlite3.Row):
    active_mode = normalize_text(str(get_rule_value(rule, "match_mode", "smart")))
    if active_mode not in {"strict", "smart"}:
        active_mode = "smart"
    logging.info("Procesando búsqueda %s (modo=%s)", rule["name"], active_mode)

    try:
        search_results = fetch_search_results(rule)
    except Exception as exc:
        logging.exception("Error cargando búsqueda %s: %s", rule["name"], exc)
        return

    stats = {
        "total": len(search_results),
        "already_seen": 0,
        "price_filtered": 0,
        "criteria_filtered": 0,
        "sent": 0,
    }
    criteria_reasons = Counter()

    for item in search_results:
        item_id = item["item_id"]
        title = item["title"]
        price = item["price"]
        url = item["url"]

        if already_seen(rule["id"], item_id):
            stats["already_seen"] += 1
            continue

        if rule["max_price"] is not None and price is not None and price > rule["max_price"]:
            stats["price_filtered"] += 1
            mark_seen(rule["id"], item_id, title, price, url)
            continue

        if rule["min_price"] is not None and price is not None and price < rule["min_price"]:
            stats["price_filtered"] += 1
            mark_seen(rule["id"], item_id, title, price, url)
            continue

        description = str(item.get("description") or "").strip()
        description_required_terms = split_csv(get_rule_value(rule, "description_must_include", ""))
        description_optional_terms = split_csv(get_rule_value(rule, "description_must_include_or", ""))
        if (description_required_terms or description_optional_terms) and not description:
            try:
                description = fetch_item_description(url)
            except Exception:
                description = ""

        matched, reasons, match_score, mode_used = evaluate_rule_match(rule, title, description)
        if not matched:
            stats["criteria_filtered"] += 1
            for reason in reasons:
                criteria_reasons[reason] += 1
            mark_seen(rule["id"], item_id, title, price, url)
            continue

        message_lines = [
            "🟢 Chollo detectado",
            f"Regla: {rule['name']}",
            f"Título: {title}",
            f"Precio: {price if price is not None else 'No detectado'} €",
            f"Modo: {'inteligente' if mode_used == 'smart' else 'estricto'}",
        ]
        if match_score is not None:
            message_lines.append(f"Score: {match_score}%")
        message_lines.append(f"URL: {url}")

        message = "\n".join(message_lines)
        ok, status = send_telegram(message)
        log_notification(rule["id"], item_id, title, price, url, status if ok else f"ERROR: {status}")
        mark_seen(rule["id"], item_id, title, price, url)
        stats["sent"] += 1
        time.sleep(2)

    if criteria_reasons:
        top_reasons = ", ".join([f"{k}={v}" for k, v in criteria_reasons.most_common(5)])
    else:
        top_reasons = "-"
    logging.info(
        "Resumen regla %s -> total=%s | vistos=%s | precio=%s | criterio=%s | enviados=%s | top_descartes=%s",
        rule["name"],
        stats["total"],
        stats["already_seen"],
        stats["price_filtered"],
        stats["criteria_filtered"],
        stats["sent"],
        top_reasons,
    )


def monitor_loop():
    while True:
        try:
            with db_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM watch_rules WHERE is_active = 1 ORDER BY id DESC")
                rules = cur.fetchall()

            for rule in rules:
                process_rule(rule)
                time.sleep(5)
        except Exception as exc:
            logging.exception("Error general del monitor: %s", exc)

        logging.info("Esperando %s segundos para la siguiente ronda", POLL_SECONDS)
        time.sleep(POLL_SECONDS)


_started = False
_monitor_lock = threading.Lock()


def start_background_monitor_once():
    global _started
    with _monitor_lock:
        if _started:
            return
        _started = True
        t = threading.Thread(target=monitor_loop, daemon=True)
        t.start()
        logging.info("Monitor en segundo plano iniciado")


# ============================================================
# TEMPLATES
# ============================================================
LOGIN_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Login - Monitor Wallapop</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 520px; margin: 60px auto; padding: 0 16px; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 20px; }
    input { width: 100%; padding: 10px; margin-top: 6px; margin-bottom: 12px; box-sizing: border-box; }
    .btn { display: inline-block; padding: 10px 14px; background: black; color: white; text-decoration: none; border-radius: 8px; border: 0; cursor: pointer; }
    .flash { padding: 10px; background: #f2f2f2; border-radius: 8px; margin-bottom: 12px; }
  </style>
</head>
<body>
  <div class="card">
    <h1>Monitor Wallapop</h1>
    <p>Acceso privado</p>
    {% with messages = get_flashed_messages() %}
      {% if messages %}
        {% for message in messages %}
          <div class="flash">{{ message }}</div>
        {% endfor %}
      {% endif %}
    {% endwith %}
    <form method="post">
      <label>Contraseña</label>
      <input name="password" type="password" required>
      <button class="btn" type="submit">Entrar</button>
    </form>
  </div>
</body>
</html>
"""

BASE_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Monitor Wallapop</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 1100px; margin: 30px auto; padding: 0 16px; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 16px; margin-bottom: 16px; }
    input, textarea, select { width: 100%; padding: 10px; margin-top: 6px; margin-bottom: 12px; box-sizing: border-box; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid #eee; padding: 10px; text-align: left; vertical-align: top; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .btn { display: inline-block; padding: 10px 14px; background: black; color: white; text-decoration: none; border-radius: 8px; border: 0; cursor: pointer; }
    .btn-secondary { background: #555; }
    .btn-danger { background: #8b0000; }
    .topbar { display: flex; gap: 10px; align-items: center; justify-content: space-between; margin-bottom: 16px; }
    .topbar-actions { display: flex; gap: 10px; flex-wrap: wrap; }
    .muted { color: #666; font-size: 14px; }
    .flash { padding: 10px; background: #f2f2f2; border-radius: 8px; margin-bottom: 12px; }
    @media (max-width: 700px) {
      .row { grid-template-columns: 1fr; }
      .topbar { flex-direction: column; align-items: flex-start; }
    }
  </style>
</head>
<body>
  <div class="topbar">
    <div>
      <h1 style="margin:0;">Monitor Wallapop</h1>
      <p class="muted">Búsquedas editables + avisos por Telegram + ejecución continua en servidor.</p>
    </div>
    <div class="topbar-actions">
      <a class="btn btn-secondary" href="/run-now">Escanear ahora</a>
      <a class="btn btn-secondary" href="/test-telegram">Probar Telegram</a>
      <a class="btn btn-secondary" href="/health" target="_blank">Health</a>
      <a class="btn btn-danger" href="/logout">Salir</a>
    </div>
  </div>

  {% with messages = get_flashed_messages() %}
    {% if messages %}
      {% for message in messages %}
        <div class="flash">{{ message }}</div>
      {% endfor %}
    {% endif %}
  {% endwith %}

  <div class="card">
    <h2>Nueva regla</h2>
    <form method="post" action="/create">
      <div class="row">
        <div>
          <label>Nombre interno</label>
          <input name="name" placeholder="ps4" required>
        </div>
        <div>
          <label>Keywords de búsqueda</label>
          <input name="keywords" placeholder="ps4" required>
        </div>
      </div>

      <div class="row">
        <div>
          <label>Precio mínimo</label>
          <input name="min_price" type="number" step="0.01" placeholder="0">
        </div>
        <div>
          <label>Precio máximo</label>
          <input name="max_price" type="number" step="0.01" placeholder="60">
        </div>
      </div>

      <div class="row">
        <div>
          <label>Modo de coincidencia</label>
          <select name="match_mode">
            <option value="strict">Estricto (todo debe encajar)</option>
            <option value="smart" selected>Inteligente (score flexible)</option>
          </select>
        </div>
        <div>
          <label>Score mínimo (modo inteligente, 0-100)</label>
          <input name="smart_min_score" type="number" min="0" max="100" step="1" placeholder="65">
        </div>
      </div>

      <div class="row">
        <div>
          <label>Palabras que deben aparecer en el título (separadas por comas)</label>
          <input name="title_must_include" placeholder="ps4, 1tb">
        </div>
        <div>
          <label>O (alternativas en título, separadas por comas)</label>
          <input name="title_must_include_or" placeholder="playstation 4, consola sony">
        </div>
      </div>

      <label>Palabras que NO deben aparecer en el título (separadas por comas)</label>
      <input name="title_must_not_include" placeholder="edición digital, averiada, sin lector">

      <div class="row">
        <div>
          <label>Palabras que deben aparecer en la descripción (separadas por comas)</label>
          <input name="description_must_include" placeholder="mando, cables, perfecto estado">
        </div>
        <div>
          <label>O (alternativas en descripción, separadas por comas)</label>
          <input name="description_must_include_or" placeholder="mando, joy con, joystick">
        </div>
      </div>

      <label>Palabras a excluir (separadas por comas)</label>
      <input name="must_not_include" placeholder="para piezas, averiada, no funciona, sin mando, solo consola">

      <button class="btn" type="submit">Crear regla</button>
    </form>
  </div>

  <div class="card">
    <h2>Reglas</h2>
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>Nombre</th>
          <th>Keywords</th>
          <th>Precio</th>
          <th>Filtros</th>
          <th>Estado</th>
          <th>Acciones</th>
        </tr>
      </thead>
      <tbody>
        {% for r in rules %}
        <tr>
          <td>{{ r['id'] }}</td>
          <td>{{ r['name'] }}</td>
          <td>{{ r['keywords'] }}</td>
          <td>{{ r['min_price'] if r['min_price'] is not none else '-' }} - {{ r['max_price'] if r['max_price'] is not none else '-' }}</td>
          <td>
            <div><strong>Modo:</strong> {{ 'Inteligente' if (r['match_mode'] or 'smart') == 'smart' else 'Estricto' }}{% if (r['match_mode'] or 'smart') == 'smart' %} (>= {{ r['smart_min_score'] if r['smart_min_score'] is not none else 65 }}%){% endif %}</div>
            <div><strong>Título:</strong> {{ r['title_must_include'] or '-' }}</div>
            <div><strong>Título (O):</strong> {{ r['title_must_include_or'] or '-' }}</div>
            <div><strong>No título:</strong> {{ r['title_must_not_include'] or '-' }}</div>
            <div><strong>Desc:</strong> {{ r['description_must_include'] or '-' }}</div>
            <div><strong>Desc (O):</strong> {{ r['description_must_include_or'] or '-' }}</div>
            <div><strong>Excluir:</strong> {{ r['must_not_include'] or '-' }}</div>
          </td>
          <td>{{ 'Activa' if r['is_active'] else 'Pausada' }}</td>
          <td>
            <a class="btn btn-secondary" href="/toggle/{{ r['id'] }}">Activar/Pausar</a>
            <a class="btn btn-danger" href="/delete/{{ r['id'] }}" onclick="return confirm('¿Eliminar esta regla?')">Eliminar</a>
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Últimas notificaciones</h2>
    <table>
      <thead>
        <tr>
          <th>Fecha</th>
          <th>Regla</th>
          <th>Título</th>
          <th>Precio</th>
          <th>Estado</th>
          <th>URL</th>
        </tr>
      </thead>
      <tbody>
        {% for n in notifications %}
        <tr>
          <td>{{ n['created_at'] }}</td>
          <td>{{ n['watch_name'] }}</td>
          <td>{{ n['title'] }}</td>
          <td>{{ n['price'] }}</td>
          <td>{{ n['status'] }}</td>
          <td><a href="{{ n['url'] }}" target="_blank">Abrir</a></td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</body>
</html>
"""


@app.before_request
def _startup():
    init_db()
    start_background_monitor_once()


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        password = request.form.get("password", "")
        if password == APP_PASSWORD:
            session["is_logged_in"] = True
            flash("Sesión iniciada")
            return redirect(url_for("index"))
        flash("Contraseña incorrecta")
    return render_template_string(LOGIN_HTML)


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada")
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM watch_rules ORDER BY id DESC")
        rules = cur.fetchall()
        cur.execute(
            """
            SELECT n.*, w.name AS watch_name
            FROM notifications_log n
            LEFT JOIN watch_rules w ON w.id = n.watch_id
            ORDER BY n.id DESC
            LIMIT 50
            """
        )
        notifications = cur.fetchall()
    return render_template_string(BASE_HTML, rules=rules, notifications=notifications)


@app.route("/create", methods=["POST"])
@login_required
def create_rule():
    name = request.form.get("name", "").strip()
    keywords = request.form.get("keywords", "").strip()
    min_price = request.form.get("min_price", "").strip()
    max_price = request.form.get("max_price", "").strip()
    match_mode = normalize_text(request.form.get("match_mode", "smart").strip())
    smart_min_score_raw = request.form.get("smart_min_score", "").strip()
    title_must_include = request.form.get("title_must_include", "").strip()
    title_must_include_or = request.form.get("title_must_include_or", "").strip()
    title_must_not_include = request.form.get("title_must_not_include", "").strip()
    description_must_include = request.form.get("description_must_include", "").strip()
    description_must_include_or = request.form.get("description_must_include_or", "").strip()
    must_not_include = request.form.get("must_not_include", "").strip()

    if not name or not keywords:
        flash("Nombre y keywords son obligatorios")
        return redirect(url_for("index"))

    if match_mode not in {"strict", "smart"}:
        match_mode = "smart"
    smart_min_score = clamp_score(smart_min_score_raw if smart_min_score_raw else 65, 65.0)

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO watch_rules (
                name, keywords, min_price, max_price,
                match_mode, smart_min_score,
                title_must_include, title_must_include_or, title_must_not_include,
                description_must_include, description_must_include_or, must_not_include, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                name,
                keywords,
                float(min_price) if min_price else None,
                float(max_price) if max_price else None,
                match_mode,
                smart_min_score,
                title_must_include or None,
                title_must_include_or or None,
                title_must_not_include or None,
                description_must_include or None,
                description_must_include_or or None,
                must_not_include or None,
            ),
        )

    flash("Regla creada correctamente")
    return redirect(url_for("index"))


@app.route("/toggle/<int:rule_id>")
@login_required
def toggle_rule(rule_id: int):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE watch_rules SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE id = ?", (rule_id,))
    flash("Regla actualizada")
    return redirect(url_for("index"))


@app.route("/delete/<int:rule_id>")
@login_required
def delete_rule(rule_id: int):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM watch_rules WHERE id = ?", (rule_id,))
    flash("Regla eliminada")
    return redirect(url_for("index"))


@app.route("/test-telegram")
@login_required
def test_telegram():
    message = "✅ Prueba de Telegram correcta desde tu Monitor Wallapop"
    ok, status = send_telegram(message)
    if ok:
        flash("Mensaje de prueba enviado a Telegram")
    else:
        flash(f"Error enviando Telegram: {status}")
    return redirect(url_for("index"))


@app.route("/run-now")
@login_required
def run_now():
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM watch_rules WHERE is_active = 1 ORDER BY id DESC")
        rules = cur.fetchall()

    for rule in rules:
        process_rule(rule)

    flash(f"Escaneo manual completado ({len(rules)} reglas activas)")
    return redirect(url_for("index"))


@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "time": datetime.utcnow().isoformat() + "Z",
        "poll_seconds": POLL_SECONDS,
        "db_path": DB_PATH,
        "monitor_started": _started,
    })


if __name__ == "__main__":
    init_db()
    start_background_monitor_once()
    app.run(host="0.0.0.0", port=PORT, debug=False)
