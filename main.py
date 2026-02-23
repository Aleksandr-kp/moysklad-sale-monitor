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

STATE_FILE = "state.json"

KW1 = "распродажа"
KW2 = "табак"

WORK_START_HOUR = 8   # 08:00 МСК
WORK_END_HOUR = 18    # до 18:00 МСК (не включая 18:00)
CHECK_SLEEP = 0.15    # пауза между страницами пагинации


# ====== TELEGRAM ======
def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        raise RuntimeError("Не заданы TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID (GitHub Secrets).")

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        json={
            "chat_id": TG_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=30,
    )
    resp.raise_for_status()


# ====== STATE ======
def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {
            "initialized": False,
            "products": {},               # pid -> {name, price_rub, category}
            "last_heartbeat_date": None,  # "YYYY-MM-DD"
        }
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ====== MOYSKLAD B2B API ======
def get_categories() -> list[dict]:
    url = f"{BASE}/{SHOP_TOKEN}/categories.json"
    r = requests.get(url, headers={"accept": "application/json"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("rows"), list):
        return data["rows"]
    return []


def _normalize_confusables(s: str) -> str:
    """
    Частая проблема: в названии категории могут оказаться латинские буквы,
    визуально похожие на кириллицу (пример: "Рacпродажа").
    Тогда поиск по "распродажа" не срабатывает.

    Здесь заменяем похожие латинские буквы на кириллицу:
    a->а, c->с, e->е, o->о, p->р, x->х, y->у, k->к, m->м, t->т, b->в
    """
    mapping = str.maketrans({
        "a": "а", "c": "с", "e": "е", "o": "о", "p": "р", "x": "х", "y": "у",
        "k": "к", "m": "м", "t": "т", "b": "в",
        "A": "А", "C": "С", "E": "Е", "O": "О", "P": "Р", "X": "Х", "Y": "У",
        "K": "К", "M": "М", "T": "Т", "B": "В",
    })
    return s.translate(mapping)


def find_sale_tobacco_categories(categories: list[dict]) -> list[dict]:
    result = []
    for c in categories:
        name = str(c.get("name", "")).strip()
        normalized = _normalize_confusables(name).lower()
        if KW1 in normalized and KW2 in normalized:
            result.append(c)
    return result


def fetch_products_page(category_id: str, limit: int, offset: int) -> dict | list:
    url = f"{BASE}/{SHOP_TOKEN}/products.json"
    params = {
        "category_id": category_id,
        "limit": limit,
        "offset": offset,
        "search": "",
    }
    r = requests.get(url, params=params, headers={"accept": "application/json"}, timeout=30)
    r.raise_for_status()
    return r.json()


def iter_products(category_id: str) -> list[dict]:
    limit = 100
    offset = 0
    all_rows: list[dict] = []

    while True:
        data = fetch_products_page(category_id, limit, offset)

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
    """
    Приводим цену к рублям (float).
    Обычно цена приходит в копейках (>= 1000), тогда /100.
    """
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

    price_rub = parse_price_to_rub(p)

    return {
        "id": pid,
        "name": name,
        "price_rub": price_rub,
        "category": category_name,
    }


# ====== TIME RULES ======
def is_work_time(now: datetime) -> bool:
    # 08:00–17:59
    return WORK_START_HOUR <= now.hour < WORK_END_HOUR


def maybe_heartbeat(state: dict, now: datetime) -> None:
    """
    Утренний сигнал "я живой".
    Окно 08:00–08:29, чтобы не зависеть от точности cron.
    """
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
    chunks = []
    cur = []
    cur_len = 0
    for line in lines:
        add_len = len(line) + 1
        if cur and cur_len + add_len > max_chars:
            chunks.append("\n".join(cur))
            cur = []
            cur_len = 0
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
                f"• [{cur['category']}] {cur['name']}: "
                f"{fmt_money(old.get('price_rub'))} → {fmt_money(cur.get('price_rub'))}"
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

    # 1) Утренний "я живой"
    maybe_heartbeat(state, now)

    # 2) Если не рабочее время — выходим (heartbeat мог отправиться)
    if not is_work_time(now):
        save_state(state)
        return

    # 3) Берём категории и ищем "распродажа + табак"
    categories = get_categories()
    target_cats = find_sale_tobacco_categories(categories)

    if not target_cats:
        tg_send("⚠️ Не нашёл категорий по фильтру 'распродажа' + 'табак'.")
        save_state(state)
        return

    # 4) Собираем товары по всем найденным категориям
    current: dict[str, dict] = {}
    cat_to_products: dict[str, list[dict]] = {}

    for c in target_cats:
        cid = str(c.get("id") or c.get("uuid") or c.get("category_id") or "").strip()
        cname = str(c.get("name") or "").strip()
        if not cid or not cname:
            continue

        raw = iter_products(cid)
        normed = []
        for p in raw:
            n = normalize_product(p, cname)
            if n:
                normed.append(n)
                current[n["id"]] = {
                    "name": n["name"],
                    "price_rub": n["price_rub"],
                    "category": n["category"],
                }

        normed.sort(key=lambda x: x["name"].lower())
        cat_to_products[cname] = normed

    # 5) Первый запуск — отправим полный список
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
