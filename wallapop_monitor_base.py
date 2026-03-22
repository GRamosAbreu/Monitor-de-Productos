import os
import re
import time
import uuid
import sqlite3
import logging
import random
import threading
import unicodedata
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter, deque
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

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


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return float(raw.strip())


load_local_env()
APP_SECRET = env_str("APP_SECRET", "cambia-esta-clave")
APP_PASSWORD = env_str("APP_PASSWORD", "cambia-esta-password")
DB_PATH = env_str("DB_PATH", "wallapop_app.db")
POLL_SECONDS = int(env_str("POLL_SECONDS", "150"))  # 2.5 minutos
TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN", "PON_AQUI_TU_BOT_TOKEN")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID", "PON_AQUI_TU_CHAT_ID")
WALLAPOP_APP_VERSION = env_str("WALLAPOP_APP_VERSION", "8.1737.0")
WALLAPOP_APP_VERSION_HEADER = env_str("WALLAPOP_APP_VERSION_HEADER", WALLAPOP_APP_VERSION.replace(".", ""))
FILTER_TODAY_ONLY = env_bool("FILTER_TODAY_ONLY", True)
PORT = int(env_str("PORT", "8080"))
LOCAL_TIMEZONE = env_str("LOCAL_TIMEZONE", "Europe/Madrid")
LOCAL_REFERENCE_LATITUDE = env_float("LOCAL_REFERENCE_LATITUDE", 37.3891)
LOCAL_REFERENCE_LONGITUDE = env_float("LOCAL_REFERENCE_LONGITUDE", -5.9845)
LOCAL_CHOLLO_MAX_KM = max(0.1, env_float("LOCAL_CHOLLO_MAX_KM", 15.0))
MAX_CONCURRENT_RULES = max(1, int(env_str("MAX_CONCURRENT_RULES", "5")))
RULE_BATCH_PAUSE_SECONDS = max(0.0, float(env_str("RULE_BATCH_PAUSE_SECONDS", "5")))
RULE_START_STAGGER_MIN_SECONDS = max(0.0, float(env_str("RULE_START_STAGGER_MIN_SECONDS", "0.35")))
RULE_START_STAGGER_MAX_SECONDS = max(
    RULE_START_STAGGER_MIN_SECONDS,
    float(env_str("RULE_START_STAGGER_MAX_SECONDS", "1.10")),
)
WALLAPOP_SEARCH_MAX_PAGES = max(1, int(env_str("WALLAPOP_SEARCH_MAX_PAGES", "2")))
RECHECK_FILTERED_AFTER_SECONDS = max(0, int(env_str("RECHECK_FILTERED_AFTER_SECONDS", "900")))
SCRAPER_HEALTH_ALERTS_ENABLED = env_bool("SCRAPER_HEALTH_ALERTS_ENABLED", True)
SCRAPER_HEALTH_WINDOW_SECONDS = max(60, int(env_str("SCRAPER_HEALTH_WINDOW_SECONDS", "900")))
SCRAPER_HEALTH_ALERT_COOLDOWN_SECONDS = max(60, int(env_str("SCRAPER_HEALTH_ALERT_COOLDOWN_SECONDS", "1800")))
SCRAPER_HEALTH_MIN_SEARCHES = max(3, int(env_str("SCRAPER_HEALTH_MIN_SEARCHES", "6")))
SCRAPER_HEALTH_MIN_PROBLEMS = max(1, int(env_str("SCRAPER_HEALTH_MIN_PROBLEMS", "3")))
SCRAPER_HEALTH_PROBLEM_RATIO = min(1.0, max(0.0, float(env_str("SCRAPER_HEALTH_PROBLEM_RATIO", "0.50"))))

try:
    LOCAL_TZ = ZoneInfo(LOCAL_TIMEZONE)
except Exception:
    logging.warning("Zona horaria invalida %s; usando UTC", LOCAL_TIMEZONE)
    LOCAL_TZ = timezone.utc

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

_CATEGORY_CACHE_TTL_SECONDS = 60 * 60 * 12
_category_cache_lock = threading.Lock()
_category_cache_expires_at = 0.0
_category_cache_data: list[dict[str, Any]] = []
_scraper_health_lock = threading.Lock()
_scraper_health_events: deque[dict[str, Any]] = deque()
_scraper_health_last_alert_at = 0.0


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
                category_id INTEGER,
                min_price REAL,
                max_price REAL,
                title_must_include TEXT,
                title_must_include_or TEXT,
                title_starts_with_any TEXT,
                title_must_not_include TEXT,
                description_must_include TEXT,
                description_must_include_or TEXT,
                description_must_not_include TEXT,
                must_not_include TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute("PRAGMA table_info(watch_rules)")
        watch_rules_columns = {row[1] for row in cur.fetchall()}
        if "title_must_include_or" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN title_must_include_or TEXT")
        if "title_starts_with_any" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN title_starts_with_any TEXT")
        if "title_must_not_include" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN title_must_not_include TEXT")
        if "description_must_include_or" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN description_must_include_or TEXT")
        if "description_must_not_include" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN description_must_not_include TEXT")
        if "category_id" not in watch_rules_columns:
            cur.execute("ALTER TABLE watch_rules ADD COLUMN category_id INTEGER")
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
        cur.execute("PRAGMA table_info(seen_items)")
        seen_items_columns = {row[1] for row in cur.fetchall()}
        if "decision" not in seen_items_columns:
            cur.execute("ALTER TABLE seen_items ADD COLUMN decision TEXT")
        if "last_checked_ts" not in seen_items_columns:
            cur.execute("ALTER TABLE seen_items ADD COLUMN last_checked_ts REAL")
        cur.execute(
            """
            UPDATE seen_items
            SET
                decision = COALESCE(decision, 'legacy_seen'),
                last_checked_ts = COALESCE(last_checked_ts, strftime('%s', 'now'))
            WHERE decision IS NULL OR last_checked_ts IS NULL
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


def first_word(text: Optional[str]) -> str:
    normalized = normalize_text(text)
    if not normalized:
        return ""

    for token in re.split(r"\s+", normalized):
        cleaned = re.sub(r"^\W+|\W+$", "", token)
        if cleaned:
            return cleaned
    return ""


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
    collapsed = re.sub(r"\s+", " ", str(text)).strip().lower()
    return "".join(
        char for char in unicodedata.normalize("NFKD", collapsed) if not unicodedata.combining(char)
    )


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


def contains_phrase(text: Optional[str], phrase: Optional[str]) -> bool:
    return contains_term(text, phrase)


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


def prune_scraper_health_events(now_ts: float):
    while _scraper_health_events and (now_ts - _scraper_health_events[0]["ts"]) > SCRAPER_HEALTH_WINDOW_SECONDS:
        _scraper_health_events.popleft()


def build_scraper_health_snapshot_from_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    total_searches = len(events)
    api_ok = sum(1 for event in events if event["outcome"] == "api_ok")
    fallback_ok = sum(1 for event in events if event["outcome"] == "fallback_ok")
    failed = sum(1 for event in events if event["outcome"] == "failed")
    blocked_http = sum(1 for event in events if event.get("status_code") in {403, 429})
    problem_count = fallback_ok + failed

    reasons: list[str] = []
    if blocked_http >= 2:
        reasons.append(f"bloqueos_http={blocked_http}")
    if failed >= 2:
        reasons.append(f"fallos={failed}")
    if (
        total_searches >= SCRAPER_HEALTH_MIN_SEARCHES
        and problem_count >= SCRAPER_HEALTH_MIN_PROBLEMS
        and (problem_count / total_searches) >= SCRAPER_HEALTH_PROBLEM_RATIO
    ):
        reasons.append(f"ratio_problemas={problem_count}/{total_searches}")

    last_event = events[-1] if events else {}
    return {
        "window_seconds": SCRAPER_HEALTH_WINDOW_SECONDS,
        "total_searches": total_searches,
        "api_ok": api_ok,
        "fallback_ok": fallback_ok,
        "failed": failed,
        "blocked_http": blocked_http,
        "problem_count": problem_count,
        "degraded": len(reasons) > 0,
        "reasons": reasons,
        "last_rule_name": last_event.get("rule_name"),
        "last_status_code": last_event.get("status_code"),
        "last_outcome": last_event.get("outcome"),
    }


def build_scraper_health_snapshot(now_ts: Optional[float] = None) -> dict[str, Any]:
    current_ts = now_ts if now_ts is not None else time.time()
    with _scraper_health_lock:
        prune_scraper_health_events(current_ts)
        events = list(_scraper_health_events)
    return build_scraper_health_snapshot_from_events(events)


def send_scraper_health_alert(snapshot: dict[str, Any]):
    message = (
        "ALERTA: salud del scraper en descenso\n"
        f"Ventana: {snapshot['window_seconds']}s\n"
        f"Busquedas: {snapshot['total_searches']}\n"
        f"API OK: {snapshot['api_ok']}\n"
        f"Fallback HTML: {snapshot['fallback_ok']}\n"
        f"Fallidas: {snapshot['failed']}\n"
        f"HTTP 403/429: {snapshot['blocked_http']}\n"
        f"Motivos: {', '.join(snapshot['reasons'])}\n"
        f"Ultima regla: {snapshot['last_rule_name'] or '-'}"
    )
    ok, status = send_telegram(message)
    if ok:
        logging.warning("Alerta de salud del scraper enviada a Telegram")
    else:
        logging.error("No se pudo enviar la alerta de salud del scraper: %s", status)


def record_scraper_search_outcome(
    rule_name: str,
    outcome: str,
    *,
    status_code: Optional[int] = None,
    result_count: Optional[int] = None,
    issue_type: str = "",
):
    global _scraper_health_last_alert_at

    if not SCRAPER_HEALTH_ALERTS_ENABLED:
        return

    now_ts = time.time()
    event = {
        "ts": now_ts,
        "rule_name": rule_name,
        "outcome": outcome,
        "status_code": status_code,
        "result_count": result_count,
        "issue_type": issue_type,
    }

    should_alert = False
    snapshot: dict[str, Any] = {}
    with _scraper_health_lock:
        _scraper_health_events.append(event)
        prune_scraper_health_events(now_ts)
        snapshot = build_scraper_health_snapshot_from_events(list(_scraper_health_events))
        if snapshot["degraded"] and (now_ts - _scraper_health_last_alert_at) >= SCRAPER_HEALTH_ALERT_COOLDOWN_SECONDS:
            _scraper_health_last_alert_at = now_ts
            should_alert = True

    if should_alert:
        send_scraper_health_alert(snapshot)


def build_search_url(rule: sqlite3.Row) -> str:
    params = build_wallapop_search_params(rule)
    return f"https://es.wallapop.com/search?{urlencode(params)}"


def build_wallapop_search_params(rule: sqlite3.Row, *, next_page: Optional[str] = None) -> dict[str, Any]:
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

    try:
        category_id = int(rule["category_id"]) if rule["category_id"] is not None else None
    except Exception:
        category_id = None
    if category_id is not None:
        params["category_id"] = category_id

    if next_page:
        params["next_page"] = next_page

    return params


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


def flatten_categories(categories: Any, parent_path: str = "") -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    if not isinstance(categories, list):
        return flattened

    for node in categories:
        if not isinstance(node, dict):
            continue

        name = str(node.get("name") or "").strip()
        if not name:
            continue

        full_label = f"{parent_path} > {name}" if parent_path else name
        try:
            category_id = int(node.get("id"))
        except Exception:
            category_id = None

        if category_id is not None:
            flattened.append({"id": category_id, "label": full_label})

        flattened.extend(flatten_categories(node.get("subcategories", []), full_label))

    return flattened


def load_wallapop_categories(force_refresh: bool = False) -> list[dict[str, Any]]:
    global _category_cache_data, _category_cache_expires_at

    now_ts = time.time()
    with _category_cache_lock:
        if (not force_refresh) and _category_cache_data and now_ts < _category_cache_expires_at:
            return list(_category_cache_data)

    loaded_categories: list[dict[str, Any]] = []
    try:
        response = requests.get(
            "https://api.wallapop.com/api/v3/categories",
            headers={
                **HEADERS,
                "Accept": "application/json, text/plain, */*",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        loaded_categories = flatten_categories(payload.get("categories", []))

        deduped: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        for category in loaded_categories:
            category_id = category.get("id")
            if not isinstance(category_id, int):
                continue
            if category_id in seen_ids:
                continue
            seen_ids.add(category_id)
            deduped.append(category)

        loaded_categories = sorted(deduped, key=lambda c: normalize_text(str(c.get("label", ""))))
    except Exception as exc:
        logging.exception("No se pudieron cargar las categorías de Wallapop: %s", exc)

    with _category_cache_lock:
        if loaded_categories:
            _category_cache_data = loaded_categories
            _category_cache_expires_at = now_ts + _CATEGORY_CACHE_TTL_SECONDS
        elif not _category_cache_data:
            _category_cache_data = []
            _category_cache_expires_at = now_ts + 300

        return list(_category_cache_data)


def extract_price_amount(item: dict[str, Any]) -> Optional[float]:
    price_data = item.get("price")
    if isinstance(price_data, dict):
        amount = price_data.get("amount")
        if isinstance(amount, (int, float)):
            return float(amount)
    if isinstance(price_data, (int, float)):
        return float(price_data)
    return None


def extract_location_coordinates(item: dict[str, Any]) -> tuple[Optional[float], Optional[float], str]:
    location = item.get("location")
    if not isinstance(location, dict):
        return None, None, ""

    try:
        latitude = float(location.get("latitude"))
    except Exception:
        latitude = None

    try:
        longitude = float(location.get("longitude"))
    except Exception:
        longitude = None

    city = str(location.get("city") or "").strip()
    region = str(location.get("region2") or location.get("region") or "").strip()
    location_parts = [part for part in [city, region] if part]
    return latitude, longitude, ", ".join(location_parts)


def haversine_distance_km(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    earth_radius_km = 6371.0
    lat1 = math.radians(latitude_a)
    lon1 = math.radians(longitude_a)
    lat2 = math.radians(latitude_b)
    lon2 = math.radians(longitude_b)
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1

    hav = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * earth_radius_km * math.asin(math.sqrt(hav))


def calculate_item_distance_km(item: dict[str, Any]) -> Optional[float]:
    latitude = item.get("location_latitude")
    longitude = item.get("location_longitude")
    if latitude is None or longitude is None:
        return None
    try:
        return haversine_distance_km(
            LOCAL_REFERENCE_LATITUDE,
            LOCAL_REFERENCE_LONGITUDE,
            float(latitude),
            float(longitude),
        )
    except Exception:
        return None


def extract_taxonomy_ids(item: dict[str, Any]) -> list[int]:
    ids: list[int] = []

    def add_candidate(value: Any):
        try:
            parsed = int(value)
        except Exception:
            return
        if parsed not in ids:
            ids.append(parsed)

    add_candidate(item.get("category_id"))

    taxonomy = item.get("taxonomy")
    if isinstance(taxonomy, list):
        for node in taxonomy:
            if isinstance(node, dict):
                add_candidate(node.get("id"))

    return ids


def is_created_today_local(created_at_ms: Any) -> bool:
    if created_at_ms is None:
        return False
    try:
        created_dt = datetime.fromtimestamp(float(created_at_ms) / 1000.0, tz=timezone.utc).astimezone(LOCAL_TZ)
    except Exception:
        return False
    return created_dt.date() == datetime.now(LOCAL_TZ).date()


def extract_wallapop_items(payload: dict[str, Any], search_url: str) -> tuple[list[dict[str, Any]], Optional[str]]:
    data = payload.get("data", {})
    section = data.get("section", {}) if isinstance(data, dict) else {}
    section_payload = section.get("payload", {}) if isinstance(section, dict) else {}
    raw_items = section_payload.get("items", []) if isinstance(section_payload, dict) else []

    items: list[dict[str, Any]] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        created_at = raw.get("created_at")
        if FILTER_TODAY_ONLY and not is_created_today_local(created_at):
            continue
        taxonomy_ids = extract_taxonomy_ids(raw)
        location_latitude, location_longitude, location_label = extract_location_coordinates(raw)

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
                "category_id": raw.get("category_id"),
                "taxonomy_ids": taxonomy_ids,
                "location_latitude": location_latitude,
                "location_longitude": location_longitude,
                "location_label": location_label,
            }
        )

    next_page = None
    for candidate in (
        section_payload.get("next_page") if isinstance(section_payload, dict) else None,
        section_payload.get("nextPage") if isinstance(section_payload, dict) else None,
        section.get("next_page") if isinstance(section, dict) else None,
        section.get("nextPage") if isinstance(section, dict) else None,
        data.get("next_page") if isinstance(data, dict) else None,
        data.get("nextPage") if isinstance(data, dict) else None,
        payload.get("next_page"),
        payload.get("nextPage"),
    ):
        candidate_str = str(candidate or "").strip()
        if candidate_str:
            next_page = candidate_str
            break

    return items, next_page


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
                "category_id": None,
                "taxonomy_ids": [],
                "location_latitude": None,
                "location_longitude": None,
                "location_label": "",
            }
        )
    return items


def fetch_search_results_with_fallback(
    rule: sqlite3.Row,
    search_url: str,
    *,
    issue_type: str,
    status_code: Optional[int] = None,
) -> list[dict]:
    try:
        items = fetch_search_results_legacy_html(search_url)
        record_scraper_search_outcome(
            rule["name"],
            "fallback_ok",
            status_code=status_code,
            result_count=len(items),
            issue_type=issue_type,
        )
        logging.info("Fallback HTML: %s resultados para %s", len(items), rule["name"])
        return items
    except Exception:
        record_scraper_search_outcome(
            rule["name"],
            "failed",
            status_code=status_code,
            issue_type=issue_type,
        )
        raise


def fetch_search_results(rule: sqlite3.Row) -> list[dict]:
    search_url = build_search_url(rule)
    api_headers = build_wallapop_api_headers(search_url)
    try:
        page_items: list[dict[str, Any]] = []
        seen_item_ids: set[str] = set()
        next_page: Optional[str] = None
        pages_loaded = 0

        while pages_loaded < WALLAPOP_SEARCH_MAX_PAGES:
            params = build_wallapop_search_params(rule, next_page=next_page)
            response = requests.get("https://api.wallapop.com/api/v3/search", params=params, headers=api_headers, timeout=30)
            response.raise_for_status()
            payload = response.json()
            items, next_page = extract_wallapop_items(payload, search_url)

            for item in items:
                item_id = item["item_id"]
                if item_id in seen_item_ids:
                    continue
                seen_item_ids.add(item_id)
                page_items.append(item)

            pages_loaded += 1
            if not next_page:
                break

        logging.info(
            "Wallapop API: %s resultados para %s (paginas=%s)",
            len(page_items),
            rule["name"],
            pages_loaded,
        )
        record_scraper_search_outcome(rule["name"], "api_ok", result_count=len(page_items))
        return page_items
    except requests.exceptions.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        logging.exception("Fallo HTTP en API de Wallapop, usando fallback HTML: %s", exc)
        return fetch_search_results_with_fallback(
            rule,
            search_url,
            issue_type=f"api_http_{status_code or 'unknown'}",
            status_code=status_code,
        )
    except Exception as exc:
        logging.exception("Fallo en API de Wallapop, usando fallback HTML: %s", exc)
        return fetch_search_results_with_fallback(rule, search_url, issue_type="api_error")


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

    include_terms = split_csv(rule["title_must_include"])
    try:
        include_or_raw = rule["title_must_include_or"]
    except Exception:
        include_or_raw = None
    include_or_terms = split_csv(include_or_raw)
    if include_terms or include_or_terms:
        include_ok = bool(include_terms) and all(contains_term(title_n, term) for term in include_terms)
        include_or_ok = bool(include_or_terms) and any(contains_term(title_n, term) for term in include_or_terms)
        if not (include_ok or include_or_ok):
            for term in include_terms:
                if not contains_term(title_n, term):
                    reasons.append(f"title:{term}")
            if include_or_terms:
                reasons.append(f"title_or:{'|'.join(include_or_terms)}")

    try:
        starts_with_raw = rule["title_starts_with_any"]
    except Exception:
        starts_with_raw = None
    starts_with_terms = split_csv(starts_with_raw)
    if starts_with_terms:
        allowed_starts = [normalize_text(term) for term in starts_with_terms if normalize_text(term)]
        if allowed_starts and not any(title_n.startswith(start) for start in allowed_starts):
            reasons.append(f"title_starts_with:{'|'.join(allowed_starts)}")

    for term in split_csv(rule["title_must_not_include"]):
        if contains_phrase(title_n, term):
            reasons.append(f"title_excluded:{term}")

    desc_include_terms = split_csv(rule["description_must_include"])
    try:
        desc_include_or_raw = rule["description_must_include_or"]
    except Exception:
        desc_include_or_raw = None
    desc_include_or_terms = split_csv(desc_include_or_raw)
    if desc_include_terms or desc_include_or_terms:
        desc_include_ok = bool(desc_include_terms) and all(contains_term(desc_n, term) for term in desc_include_terms)
        desc_include_or_ok = bool(desc_include_or_terms) and any(contains_term(desc_n, term) for term in desc_include_or_terms)
        if not (desc_include_ok or desc_include_or_ok):
            for term in desc_include_terms:
                if not contains_term(desc_n, term):
                    reasons.append(f"description:{term}")
            if desc_include_or_terms:
                reasons.append(f"description_or:{'|'.join(desc_include_or_terms)}")

    for term in split_csv(rule["description_must_not_include"]):
        if contains_phrase(desc_n, term):
            reasons.append(f"description_excluded:{term}")

    for term in split_csv(rule["must_not_include"]):
        if contains_phrase(whole, term):
            reasons.append(f"excluded:{term}")

    return len(reasons) == 0, reasons


def rule_matches(rule: sqlite3.Row, title: str, description: str) -> bool:
    ok, _ = rule_match_details(rule, title, description)
    return ok


def should_skip_item(watch_id: int, item_id: str) -> bool:
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT decision, last_checked_ts FROM seen_items WHERE watch_id = ? AND item_id = ?",
            (watch_id, item_id),
        )
        row = cur.fetchone()
        if row is None:
            return False

        decision = str(row["decision"] or "").strip().lower()
        if decision in {"notified", "legacy_seen"}:
            return True

        try:
            last_checked_ts = float(row["last_checked_ts"])
        except Exception:
            return False

        return (time.time() - last_checked_ts) < RECHECK_FILTERED_AFTER_SECONDS


def record_item_decision(
    watch_id: int,
    item_id: str,
    title: str,
    price: Optional[float],
    url: str,
    decision: str,
):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO seen_items (item_id, watch_id, title, price, url, decision, last_checked_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id, watch_id) DO UPDATE SET
                title = excluded.title,
                price = excluded.price,
                url = excluded.url,
                decision = excluded.decision,
                last_checked_ts = excluded.last_checked_ts,
                seen_at = CURRENT_TIMESTAMP
            """,
            (item_id, watch_id, title, price, url, decision, time.time()),
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
    logging.info("Procesando búsqueda %s", rule["name"])
    try:
        search_results = fetch_search_results(rule)
    except Exception as exc:
        logging.exception("Error cargando búsqueda %s: %s", rule["name"], exc)
        return

    try:
        rule_category_id = int(rule["category_id"]) if rule["category_id"] is not None else None
    except Exception:
        rule_category_id = None

    stats = {
        "total": len(search_results),
        "already_seen": 0,
        "price_filtered": 0,
        "category_filtered": 0,
        "criteria_filtered": 0,
        "telegram_failed": 0,
        "sent": 0,
    }
    criteria_reasons = Counter()

    for item in search_results:
        item_id = item["item_id"]
        title = item["title"]
        price = item["price"]
        url = item["url"]
        distance_km = calculate_item_distance_km(item)
        is_local_chollo = distance_km is not None and distance_km <= LOCAL_CHOLLO_MAX_KM

        item_category_ids: set[int] = set()
        for raw_category in item.get("taxonomy_ids", []):
            try:
                item_category_ids.add(int(raw_category))
            except Exception:
                continue

        if should_skip_item(rule["id"], item_id):
            stats["already_seen"] += 1
            continue

        if rule_category_id is not None and rule_category_id not in item_category_ids:
            stats["category_filtered"] += 1
            stats["criteria_filtered"] += 1
            criteria_reasons[f"category:{rule_category_id}"] += 1
            record_item_decision(rule["id"], item_id, title, price, url, "filtered_category")
            continue

        if rule["max_price"] is not None and price is not None and price > rule["max_price"]:
            stats["price_filtered"] += 1
            record_item_decision(rule["id"], item_id, title, price, url, "filtered_price")
            continue

        if rule["min_price"] is not None and price is not None and price < rule["min_price"]:
            stats["price_filtered"] += 1
            record_item_decision(rule["id"], item_id, title, price, url, "filtered_price")
            continue

        description = str(item.get("description") or "").strip()
        description_required_terms = split_csv(rule["description_must_include"])
        try:
            description_optional_terms = split_csv(rule["description_must_include_or"])
        except Exception:
            description_optional_terms = []
        try:
            description_excluded_terms = split_csv(rule["description_must_not_include"])
        except Exception:
            description_excluded_terms = []
        global_excluded_terms = split_csv(rule["must_not_include"])

        if (
            description_required_terms
            or description_optional_terms
            or description_excluded_terms
            or global_excluded_terms
        ) and not description:
            try:
                description = fetch_item_description(url)
            except Exception:
                description = ""

        matched, reasons = rule_match_details(rule, title, description)
        if not matched:
            stats["criteria_filtered"] += 1
            for reason in reasons:
                criteria_reasons[reason] += 1
            record_item_decision(rule["id"], item_id, title, price, url, "filtered_criteria")
            continue

        header = "🔴 Chollo local detectado" if is_local_chollo else "🟢 Chollo detectado"
        message_lines = [
            header,
            f"Regla: {rule['name']}",
            f"Título: {title}",
            f"Precio: {price if price is not None else 'No detectado'} €",
        ]
        if distance_km is not None:
            distance_text = f"{distance_km:.1f} km"
            location_label = str(item.get("location_label") or "").strip()
            if location_label:
                distance_text = f"{distance_text} ({location_label})"
            message_lines.append(f"Distancia: {distance_text}")
        message_lines.append(f"URL: {url}")
        message = "\n".join(message_lines)
        ok, status = send_telegram(message)
        log_notification(rule["id"], item_id, title, price, url, status if ok else f"ERROR: {status}")
        if ok:
            record_item_decision(rule["id"], item_id, title, price, url, "notified")
            stats["sent"] += 1
        else:
            record_item_decision(rule["id"], item_id, title, price, url, "send_failed")
            stats["telegram_failed"] += 1
        time.sleep(2)

    if criteria_reasons:
        top_reasons = ", ".join([f"{k}={v}" for k, v in criteria_reasons.most_common(5)])
    else:
        top_reasons = "-"
    logging.info(
        "Resumen regla %s -> total=%s | vistos=%s | precio=%s | categoria=%s | criterio=%s | telegram_failed=%s | enviados=%s | top_descartes=%s",
        rule["name"],
        stats["total"],
        stats["already_seen"],
        stats["price_filtered"],
        stats["category_filtered"],
        stats["criteria_filtered"],
        stats["telegram_failed"],
        stats["sent"],
        top_reasons,
    )


def batched_rules(rules: list[sqlite3.Row], batch_size: int) -> list[list[sqlite3.Row]]:
    return [rules[i:i + batch_size] for i in range(0, len(rules), batch_size)]


def process_rule_with_stagger(rule: sqlite3.Row, slot_index: int):
    if slot_index > 0:
        delay = random.uniform(RULE_START_STAGGER_MIN_SECONDS, RULE_START_STAGGER_MAX_SECONDS) * slot_index
        logging.info(
            "Regla %s espera %.2fs antes de lanzar su bÃºsqueda en paralelo",
            rule["name"],
            delay,
        )
        time.sleep(delay)
    process_rule(rule)


def process_rules_round(rules: list[sqlite3.Row]):
    if not rules:
        return

    total_batches = (len(rules) + MAX_CONCURRENT_RULES - 1) // MAX_CONCURRENT_RULES
    for batch_index, batch in enumerate(batched_rules(rules, MAX_CONCURRENT_RULES), start=1):
        logging.info(
            "Lanzando lote %s/%s con %s regla(s) en paralelo (mÃ¡ximo=%s)",
            batch_index,
            total_batches,
            len(batch),
            MAX_CONCURRENT_RULES,
        )

        with ThreadPoolExecutor(max_workers=len(batch), thread_name_prefix="rule-worker") as executor:
            futures = [
                executor.submit(process_rule_with_stagger, rule, slot_index)
                for slot_index, rule in enumerate(batch)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logging.exception("Fallo inesperado procesando lote paralelo: %s", exc)

        if batch_index < total_batches and RULE_BATCH_PAUSE_SECONDS > 0:
            logging.info(
                "Lote %s completado. Pausa de %.2fs antes del siguiente lote",
                batch_index,
                RULE_BATCH_PAUSE_SECONDS,
            )
            time.sleep(RULE_BATCH_PAUSE_SECONDS)


def monitor_loop():
    while True:
        try:
            with db_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM watch_rules WHERE is_active = 1 ORDER BY id DESC")
                rules = list(cur.fetchall())

            process_rules_round(rules)
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

      <label>Categoría (opcional)</label>
      <input id="create-category-search" type="text" placeholder="Buscar categoría (ej: consolas)">
      <select id="create-category-id" name="category_id">
        <option value="">Todas las categorías</option>
        {% for c in categories %}
          <option value="{{ c['id'] }}">{{ c['label'] }}</option>
        {% endfor %}
      </select>

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
          <label>Palabras que deben aparecer en el título (separadas por comas)</label>
          <input name="title_must_include" placeholder="ps4, 1tb">
        </div>
        <div>
          <label>O (alternativas en título, separadas por comas)</label>
          <input name="title_must_include_or" placeholder="playstation 4, consola sony">
        </div>
      </div>

      <label>Palabras con las que debe comenzar el titulo (separadas por comas)</label>
      <input name="title_starts_with_any" placeholder="ps4, playstation, consola">

      <label>Palabras o frases que NO deben aparecer en el título (separadas por comas)</label>
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

      <label>Palabras o frases que NO deben aparecer en la descripción (separadas por comas)</label>
      <input name="description_must_not_include" placeholder="sin mando, para piezas, con drift">

      <label>Palabras o frases a excluir en general (título o descripción, separadas por comas)</label>
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
            <div><strong>Categoría:</strong> {{ r['category_label'] or '-' }}</div>
            <div><strong>Título:</strong> {{ r['title_must_include'] or '-' }}</div>
            <div><strong>Título (O):</strong> {{ r['title_must_include_or'] or '-' }}</div>
            <div><strong>Inicio titulo:</strong> {{ r['title_starts_with_any'] or '-' }}</div>
            <div><strong>No título:</strong> {{ r['title_must_not_include'] or '-' }}</div>
            <div><strong>Desc:</strong> {{ r['description_must_include'] or '-' }}</div>
            <div><strong>Desc (O):</strong> {{ r['description_must_include_or'] or '-' }}</div>
            <div><strong>No desc:</strong> {{ r['description_must_not_include'] or '-' }}</div>
            <div><strong>Excluir global:</strong> {{ r['must_not_include'] or '-' }}</div>
          </td>
          <td>{{ 'Activa' if r['is_active'] else 'Pausada' }}</td>
          <td>
            <a class="btn btn-secondary" href="/edit/{{ r['id'] }}">Editar</a>
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

  <script>
    function wireCategorySearch(searchId, selectId) {
      const searchInput = document.getElementById(searchId);
      const select = document.getElementById(selectId);
      if (!searchInput || !select) {
        return;
      }

      const allOptions = Array.from(select.options).map((opt) => ({
        value: opt.value,
        label: opt.textContent,
      }));

      function rebuildOptions(query) {
        const selectedValue = select.value;
        const q = (query || '').toLowerCase().trim();

        select.innerHTML = '';
        allOptions.forEach((option, index) => {
          if (index === 0 || !q || option.label.toLowerCase().includes(q)) {
            const el = document.createElement('option');
            el.value = option.value;
            el.textContent = option.label;
            if (option.value === selectedValue) {
              el.selected = true;
            }
            select.appendChild(el);
          }
        });

        if (!Array.from(select.options).some((opt) => opt.selected)) {
          select.selectedIndex = 0;
        }
      }

      searchInput.addEventListener('input', () => rebuildOptions(searchInput.value));
    }

    wireCategorySearch('create-category-search', 'create-category-id');
  </script>
</body>
</html>
"""

EDIT_RULE_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Editar regla - Monitor Wallapop</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 900px; margin: 30px auto; padding: 0 16px; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 16px; margin-bottom: 16px; }
    input, textarea, select { width: 100%; padding: 10px; margin-top: 6px; margin-bottom: 12px; box-sizing: border-box; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .btn { display: inline-block; padding: 10px 14px; background: black; color: white; text-decoration: none; border-radius: 8px; border: 0; cursor: pointer; }
    .btn-secondary { background: #555; }
    .flash { padding: 10px; background: #f2f2f2; border-radius: 8px; margin-bottom: 12px; }
    @media (max-width: 700px) {
      .row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="card">
    <h1 style="margin-top:0;">Editar regla #{{ rule['id'] }}</h1>

    {% with messages = get_flashed_messages() %}
      {% if messages %}
        {% for message in messages %}
          <div class="flash">{{ message }}</div>
        {% endfor %}
      {% endif %}
    {% endwith %}

    <form method="post" action="/edit/{{ rule['id'] }}">
      <div class="row">
        <div>
          <label>Nombre interno</label>
          <input name="name" value="{{ rule['name'] or '' }}" required>
        </div>
        <div>
          <label>Keywords de búsqueda</label>
          <input name="keywords" value="{{ rule['keywords'] or '' }}" required>
        </div>
      </div>

      <label>Categoría (opcional)</label>
      <input id="edit-category-search" type="text" placeholder="Buscar categoría (ej: consolas)">
      <select id="edit-category-id" name="category_id">
        <option value="" {% if selected_category_id is none %}selected{% endif %}>Todas las categorías</option>
        {% for c in categories %}
          <option value="{{ c['id'] }}" {% if selected_category_id == c['id'] %}selected{% endif %}>{{ c['label'] }}</option>
        {% endfor %}
      </select>

      <div class="row">
        <div>
          <label>Precio mínimo</label>
          <input name="min_price" type="number" step="0.01" value="{{ '' if rule['min_price'] is none else rule['min_price'] }}">
        </div>
        <div>
          <label>Precio máximo</label>
          <input name="max_price" type="number" step="0.01" value="{{ '' if rule['max_price'] is none else rule['max_price'] }}">
        </div>
      </div>

      <div class="row">
        <div>
          <label>Palabras que deben aparecer en el título (separadas por comas)</label>
          <input name="title_must_include" value="{{ rule['title_must_include'] or '' }}">
        </div>
        <div>
          <label>O (alternativas en título, separadas por comas)</label>
          <input name="title_must_include_or" value="{{ rule['title_must_include_or'] or '' }}">
        </div>
      </div>

      <label>Palabras con las que debe comenzar el titulo (separadas por comas)</label>
      <input name="title_starts_with_any" value="{{ rule['title_starts_with_any'] or '' }}">

      <label>Palabras o frases que NO deben aparecer en el título (separadas por comas)</label>
      <input name="title_must_not_include" value="{{ rule['title_must_not_include'] or '' }}">

      <div class="row">
        <div>
          <label>Palabras que deben aparecer en la descripción (separadas por comas)</label>
          <input name="description_must_include" value="{{ rule['description_must_include'] or '' }}">
        </div>
        <div>
          <label>O (alternativas en descripción, separadas por comas)</label>
          <input name="description_must_include_or" value="{{ rule['description_must_include_or'] or '' }}">
        </div>
      </div>

      <label>Palabras o frases que NO deben aparecer en la descripción (separadas por comas)</label>
      <input name="description_must_not_include" value="{{ rule['description_must_not_include'] or '' }}">

      <label>Palabras o frases a excluir en general (título o descripción, separadas por comas)</label>
      <input name="must_not_include" value="{{ rule['must_not_include'] or '' }}">

      <button class="btn" type="submit">Guardar cambios</button>
      <a class="btn btn-secondary" href="/">Volver</a>
    </form>
  </div>

  <script>
    function wireCategorySearch(searchId, selectId) {
      const searchInput = document.getElementById(searchId);
      const select = document.getElementById(selectId);
      if (!searchInput || !select) {
        return;
      }

      const allOptions = Array.from(select.options).map((opt) => ({
        value: opt.value,
        label: opt.textContent,
      }));

      function rebuildOptions(query) {
        const selectedValue = select.value;
        const q = (query || '').toLowerCase().trim();

        select.innerHTML = '';
        allOptions.forEach((option, index) => {
          if (index === 0 || !q || option.label.toLowerCase().includes(q)) {
            const el = document.createElement('option');
            el.value = option.value;
            el.textContent = option.label;
            if (option.value === selectedValue) {
              el.selected = true;
            }
            select.appendChild(el);
          }
        });

        if (!Array.from(select.options).some((opt) => opt.selected)) {
          select.selectedIndex = 0;
        }
      }

      searchInput.addEventListener('input', () => rebuildOptions(searchInput.value));
    }

    wireCategorySearch('edit-category-search', 'edit-category-id');
  </script>
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
    categories = load_wallapop_categories()
    category_map = {int(c["id"]): c["label"] for c in categories if isinstance(c.get("id"), int)}

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM watch_rules ORDER BY id DESC")
        raw_rules = cur.fetchall()

        rules: list[dict[str, Any]] = []
        for row in raw_rules:
            rule = dict(row)
            try:
                selected_category_id = int(rule.get("category_id")) if rule.get("category_id") is not None else None
            except Exception:
                selected_category_id = None
            rule["category_label"] = category_map.get(selected_category_id, "-") if selected_category_id is not None else "-"
            rules.append(rule)

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

    return render_template_string(BASE_HTML, rules=rules, notifications=notifications, categories=categories)


@app.route("/create", methods=["POST"])
@login_required
def create_rule():
    name = request.form.get("name", "").strip()
    keywords = request.form.get("keywords", "").strip()
    category_id_raw = request.form.get("category_id", "").strip()
    min_price = request.form.get("min_price", "").strip()
    max_price = request.form.get("max_price", "").strip()
    title_must_include = request.form.get("title_must_include", "").strip()
    title_must_include_or = request.form.get("title_must_include_or", "").strip()
    title_starts_with_any = request.form.get("title_starts_with_any", "").strip()
    title_must_not_include = request.form.get("title_must_not_include", "").strip()
    description_must_include = request.form.get("description_must_include", "").strip()
    description_must_include_or = request.form.get("description_must_include_or", "").strip()
    description_must_not_include = request.form.get("description_must_not_include", "").strip()
    must_not_include = request.form.get("must_not_include", "").strip()

    if not name or not keywords:
        flash("Nombre y keywords son obligatorios")
        return redirect(url_for("index"))

    category_id: Optional[int] = None
    if category_id_raw:
        try:
            category_id = int(category_id_raw)
        except ValueError:
            flash("Categoría no válida")
            return redirect(url_for("index"))

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO watch_rules (
                name, keywords, category_id, min_price, max_price,
                title_must_include, title_must_include_or, title_starts_with_any, title_must_not_include,
                description_must_include, description_must_include_or, description_must_not_include, must_not_include, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                name,
                keywords,
                category_id,
                float(min_price) if min_price else None,
                float(max_price) if max_price else None,
                title_must_include or None,
                title_must_include_or or None,
                title_starts_with_any or None,
                title_must_not_include or None,
                description_must_include or None,
                description_must_include_or or None,
                description_must_not_include or None,
                must_not_include or None,
            ),
        )

    flash("Regla creada correctamente")
    return redirect(url_for("index"))


@app.route("/edit/<int:rule_id>", methods=["GET", "POST"])
@login_required
def edit_rule(rule_id: int):
    categories = load_wallapop_categories()

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM watch_rules WHERE id = ?", (rule_id,))
        row = cur.fetchone()

    if row is None:
        flash("Regla no encontrada")
        return redirect(url_for("index"))

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        keywords = request.form.get("keywords", "").strip()
        category_id_raw = request.form.get("category_id", "").strip()
        min_price = request.form.get("min_price", "").strip()
        max_price = request.form.get("max_price", "").strip()
        title_must_include = request.form.get("title_must_include", "").strip()
        title_must_include_or = request.form.get("title_must_include_or", "").strip()
        title_starts_with_any = request.form.get("title_starts_with_any", "").strip()
        title_must_not_include = request.form.get("title_must_not_include", "").strip()
        description_must_include = request.form.get("description_must_include", "").strip()
        description_must_include_or = request.form.get("description_must_include_or", "").strip()
        description_must_not_include = request.form.get("description_must_not_include", "").strip()
        must_not_include = request.form.get("must_not_include", "").strip()

        if not name or not keywords:
            flash("Nombre y keywords son obligatorios")
            return redirect(url_for("edit_rule", rule_id=rule_id))

        category_id: Optional[int] = None
        if category_id_raw:
            try:
                category_id = int(category_id_raw)
            except ValueError:
                flash("Categoría no válida")
                return redirect(url_for("edit_rule", rule_id=rule_id))

        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE watch_rules
                SET
                    name = ?,
                    keywords = ?,
                    category_id = ?,
                    min_price = ?,
                    max_price = ?,
                    title_must_include = ?,
                    title_must_include_or = ?,
                    title_starts_with_any = ?,
                    title_must_not_include = ?,
                    description_must_include = ?,
                    description_must_include_or = ?,
                    description_must_not_include = ?,
                    must_not_include = ?
                WHERE id = ?
                """,
                (
                    name,
                    keywords,
                    category_id,
                    float(min_price) if min_price else None,
                    float(max_price) if max_price else None,
                    title_must_include or None,
                    title_must_include_or or None,
                    title_starts_with_any or None,
                    title_must_not_include or None,
                    description_must_include or None,
                    description_must_include_or or None,
                    description_must_not_include or None,
                    must_not_include or None,
                    rule_id,
                ),
            )

        flash("Regla actualizada correctamente")
        return redirect(url_for("index"))

    rule = dict(row)
    try:
        selected_category_id = int(rule.get("category_id")) if rule.get("category_id") is not None else None
    except Exception:
        selected_category_id = None

    return render_template_string(
        EDIT_RULE_HTML,
        rule=rule,
        categories=categories,
        selected_category_id=selected_category_id,
    )

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
        rules = list(cur.fetchall())

    process_rules_round(rules)

    flash(f"Escaneo manual completado ({len(rules)} reglas activas)")
    return redirect(url_for("index"))


@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "time": datetime.utcnow().isoformat() + "Z",
        "poll_seconds": POLL_SECONDS,
        "max_concurrent_rules": MAX_CONCURRENT_RULES,
        "rule_batch_pause_seconds": RULE_BATCH_PAUSE_SECONDS,
        "scraper_health": build_scraper_health_snapshot(),
        "db_path": DB_PATH,
        "monitor_started": _started,
    })


if __name__ == "__main__":
    init_db()
    start_background_monitor_once()
    app.run(host="0.0.0.0", port=PORT, debug=False)

