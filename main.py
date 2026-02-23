import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

# ====== НАСТРОЙКИ ======
MSK = ZoneInfo("Europe/Moscow")

BASE = "https://b2b.moysklad.ru/desktop-api"
SHOP_TOKEN = os.getenv("MOYSKLAD_SHOP_TOKEN", "rqCe1pSHFAhL")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ВАЖНО: cookie с портала (GitHub Secret)
MOYSKLAD_COOKIE = os.getenv("MOYSKLAD_COOKIE", "").strip()

STATE_FILE = "state.json"

KW1 = "распродажа"
KW2 = "табак"

WORK_START_HOUR = 8
WORK_END_HOUR = 18
CHECK_SLEEP = 0.15

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
REFERER = f"https://b2b.moysklad.ru/{SHOP_TOKEN}/catalog"


# ====== TELEGRAM ======
def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        raise RuntimeError("Не заданы TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID (GitHub Secrets).")

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        json={"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True},
        timeout=30,
    )
    resp.raise_for_status()


# ====== STATE ======
def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"initialized": False, "products": {}, "last_heartbeat_date": None}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ====== HTTP SESSION ======
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "accept": "application/json, text/plain, */*",
            "user-agent": UA,
            "referer": REFERER,
            "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
    # если cookie задан — прокидываем
    if MOYSKLAD_COOKIE:
        s.headers["cookie"] = MOYSKLAD_COOKIE
    return s


# ====== MOYSKLAD B2B API ======
def get_categories(sess: requests.Session) -> list[dict]:
    url = f"{BASE}/{SHOP_TOKEN}/categories.json"
    r = sess.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("rows"), list):
        return data["rows"]
    return []


def _normalize_confusables(s: str) -> str:
    mapping = str.maketrans(
        {
            "a": "а",
            "c": "с",
            "e": "е",
            "o": "о",
            "p": "р",
            "x": "х",
            "y": "у",
            "k": "к",
            "m": "м",
            "t": "т",
            "b": "в",
            "A": "А",
            "C": "С",
            "E": "Е",
            "O": "О",
            "P": "Р",
            "X": "Х",
            "Y": "У",
            "K": "К",
            "M": "М",
            "T": "Т",
            "B": "В",
        }
    )
    return s.translate(mapping)


def find_sale_tobacco_categories(categories: list[dict]) -> list[dict]:
    result = []
    for c in categories:
        name = str(c.get("name", "")).strip()
        normalized = _normalize_confusables(name).lower()
        if KW1 in normalized and KW2 in normalized:
            result.append(c)
    return result


def fetch_products_page(
    sess: requests.Session, category_id: str, category_name: str, limit: int, offset: int
) -> dict | list:
    url = f"{BASE}/{SHOP_TOKEN}/products.json"
    params = {
        "category": category_name,  # как в твоём curl
        "category_id": category_id,
        "limit": limit,
        "offset": offset,
        "search": "",
    }
    r = sess.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def iter_products(sess: requests.Session, category_id: str, category_name: str) -> list[dict]:
    limit = 100
    offset = 0
    all_rows: list[dict] = []

    while True:
        data = fetch_products_page(sess, category_id, category_name, limit, offset)

        if isinstance(data, dict):
            rows = data.get("rows") or data.get("items") or data.get("data") or []
        elif isinstance(data, list):
            rows = data
        else:
            rows = []

        if not rows:
            break

        rows = [x for x in rows if isinstance(x, dict)]
        all_rows.extend(rows)

        if len(rows) < limit:
            break

        offset += limit
        time.sleep(CHECK_SLEEP)

    return all_rows


def parse_price_to_rub(p: dict) -> float | None:
    candidates = []
    if "price" in p:
        candidates.append(p.get("price"))
    for k in ("salePrice", "minPrice", "retail_price", "retailPrice", "price_value", "priceValue"):
        if k in p:
            candidates.append(p.get(k))

    value = None
    for c in candidates:
        if isinstance(c, dict) and "value" in c:
            value = c.get("value")
            break
        if isinstance(c, (int, float)):
            value = c
            break

    if value is None:
        return None

    try:
        v = float(value)
    except Exception:
        return None

    if v >= 1000:
        return round(v / 100.0, 2)
    return round(v, 2)


def normalize_product(p: dict, category_name: str) -> dict | None:
    pid = str(p.get("id") or p.get("uuid") or p.get("product_id") or "").strip()
    name = str(p.get("name") or p.get("title") or "").strip()
    if not pid or not name:
        return None
    return {"id": pid, "name": name, "price_rub": parse_price_to_rub(p), "category": category_name}


# ====== TIME RULES ======
def is_work_time(now: datetime) -> bool:
    return WORK_START_HOUR <= now.hour < WORK_END_HOUR


def maybe_heartbeat(state: dict, now: datetime) -> None:
    today = now.date().isoformat()
    if now.hour == WORK_START_HOUR and now.minute < 30:
        if state.get("last_heartbeat_date") != today:
            tg_send("✅ Бот работает. Мониторинг: 08:00–18:00 МСК, каждые 30 минут.")
            state["last_heartbeat_date"] = today


# ====== MESSAGE FORMAT ======
def fmt_money(price_rub: float | None) -> str:
    if price_rub is None:
        return "цена не найдена"
    s = f"{price_rub:,.2f}".replace(",", " ")
    return f"{s} ₽"


def chunk_lines(lines: list[str], max_chars: int = 3500) -> list[str]:
    chunks, cur, cur_len = [], [], 0
    for line in lines:
        add_len = len(line) + 1
        if cur and cur_len + add_len > max_chars:
            chunks.append("\n".join(cur))
            cur, cur_len = [], 0
        cur.append(line)
        cur_len += add_len
    if cur:
        chunks.append("\n".join(cur))
    return chunks


def send_full_list(cat_to_products: dict[str, list[dict]]) -> None:
    lines = ["🧾 Актуальный список (категории: распродажа + табак):"]
    for cat, items in cat_to_products.items():
        lines.append("")
        lines.append(f"📁 {cat} — {len(items)} шт.")
        for x in items:
            lines.append(f"• {x['name']} — {fmt_money(x['price_rub'])}")

    for msg in chunk_lines(lines):
        tg_send(msg)


def send_changes(added: list[dict], changed: list[tuple[dict, dict]]) -> None:
    msgs = []

    if added:
        lines = [f"🆕 Добавили ({len(added)}):"]
        for x in added[:60]:
            lines.append(f"• [{x['category']}] {x['name']} — {fmt_money(x['price_rub'])}")
        if len(added) > 60:
            lines.append(f"...и ещё {len(added) - 60}")
        msgs.extend(chunk_lines(lines))

    if changed:
        lines = [f"💸 Цена изменилась ({len(changed)}):"]
        for old, cur in changed[:60]:
            lines.append(
                f"• [{cur['category']}] {cur['name']}: {fmt_money(old.get('price_rub'))} → {fmt_money(cur.get('price_rub'))}"
            )
        if len(changed) > 60:
            lines.append(f"...и ещё {len(changed) - 60}")
        msgs.extend(chunk_lines(lines))

    for m in msgs:
        tg_send(m)


# ====== MAIN ======
def main() -> None:
    now = datetime.now(MSK)
    state = load_state()

    maybe_heartbeat(state, now)

    if not is_work_time(now):
        save_state(state)
        return

    sess = make_session()

    categories = get_categories(sess)
    target_cats = find_sale_tobacco_categories(categories)

    if not target_cats:
        tg_send("⚠️ Не нашёл категорий по фильтру 'распродажа' + 'табак'.")
        save_state(state)
        return

    current: dict[str, dict] = {}
    cat_to_products: dict[str, list[dict]] = {}

    for c in target_cats:
        cid = str(c.get("id") or c.get("uuid") or c.get("category_id") or "").strip()
        cname = str(c.get("name") or "").strip()
        if not cid or not cname:
            continue

        raw = iter_products(sess, cid, cname)
        normed = []
        for p in raw:
            n = normalize_product(p, cname)
            if n:
                normed.append(n)
                current[n["id"]] = {"name": n["name"], "price_rub": n["price_rub"], "category": n["category"]}

        normed.sort(key=lambda x: x["name"].lower())
        cat_to_products[cname] = normed

    if not state.get("initialized"):
        send_full_list(cat_to_products)
        state["initialized"] = True
        state["products"] = current
        save_state(state)
        return

    prev: dict[str, dict] = state.get("products", {})

    added: list[dict] = []
    changed: list[tuple[dict, dict]] = []

    for pid, cur in current.items():
        if pid not in prev:
            added.append(cur)
        else:
            old = prev[pid]
            if old.get("price_rub") != cur.get("price_rub"):
                changed.append((old, cur))

    state["products"] = current
    save_state(state)

    if added or changed:
        added.sort(key=lambda x: x["name"].lower())
        changed.sort(key=lambda pair: pair[1]["name"].lower())
        send_changes(added, changed)


if __name__ == "__main__":
    main()
