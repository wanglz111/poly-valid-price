#!/usr/bin/env python3
import argparse
import json
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

import pymysql
from table_naming import TableNameResolver, parse_bool_flag, parse_timezone_offset

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
DEFAULT_TABLE = "polymarket_prices"
SYMBOLS_DEFAULT = "btc,eth,sol"
CHAINLINK_UNIT_SCALE = 10**18
DEFAULT_TABLE_DATE_TZ = "+08:00"


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if value and value[0] == value[-1] and value[0] in ("\"", "'"):
                value = value[1:-1]
            os.environ.setdefault(key, value)


def http_json(url: str, timeout: float = 0.8):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def aligned_window_start(ts: int) -> int:
    return (ts // 300) * 300


def wait_until_next_second() -> int:
    now = time.time()
    next_sec = int(now) + 1
    time.sleep(max(0.0, next_sec - now))
    return next_sec


def fetch_market_by_slug(slug: str, timeout: float):
    return http_json(f"{GAMMA_BASE}/markets/slug/{urllib.parse.quote(slug)}", timeout=timeout)


def parse_up_token_id(market) -> str:
    outcomes_raw = market.get("outcomes")
    token_ids_raw = market.get("clobTokenIds")
    if not outcomes_raw or not token_ids_raw:
        raise ValueError("market payload missing outcomes/clobTokenIds")

    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
    token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw

    for idx, outcome in enumerate(outcomes):
        if str(outcome).lower() == "up":
            return str(token_ids[idx])
    raise ValueError("cannot find Up outcome")


def fetch_up_buy_price(token_id: str, timeout: float) -> Decimal:
    payload = http_json(
        f"{CLOB_BASE}/price?token_id={urllib.parse.quote(token_id)}&side=buy",
        timeout=timeout,
    )
    if payload.get("price") is None:
        raise RuntimeError(f"price response missing price: {payload}")
    return Decimal(str(payload["price"]))


def to_chainlink_unit(price: Decimal) -> int:
    scaled = (price * Decimal(CHAINLINK_UNIT_SCALE)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(scaled)


def mysql_connect(args):
    return pymysql.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_table(conn, table: str):
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      captured_at_ts BIGINT NOT NULL,
      symbol VARCHAR(16) NOT NULL,
      market_slug VARCHAR(128) NOT NULL,
      market_start_ts BIGINT NOT NULL,
      market_end_ts BIGINT NOT NULL,
      up_token_id VARCHAR(128) NOT NULL,
      up_buy_price DECIMAL(30,18) NOT NULL,
      up_buy_price_chainlink_unit_raw DECIMAL(38,0) NOT NULL,
      chainlink_price_unit_scale INT NOT NULL DEFAULT 18,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uq_symbol_market_capture (symbol, market_slug, captured_at_ts),
      KEY idx_symbol_window (symbol, market_start_ts, market_end_ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def insert_row(conn, table: str, row: dict):
    sql = f"""
    INSERT INTO `{table}` (
      captured_at_ts,
      symbol,
      market_slug,
      market_start_ts,
      market_end_ts,
      up_token_id,
      up_buy_price,
      up_buy_price_chainlink_unit_raw,
      chainlink_price_unit_scale
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      up_token_id = VALUES(up_token_id),
      up_buy_price = VALUES(up_buy_price),
      up_buy_price_chainlink_unit_raw = VALUES(up_buy_price_chainlink_unit_raw),
      chainlink_price_unit_scale = VALUES(chainlink_price_unit_scale)
    """
    params = (
        row["captured_at_ts"],
        row["symbol"],
        row["market_slug"],
        row["market_start_ts"],
        row["market_end_ts"],
        row["up_token_id"],
        str(row["up_buy_price"]),
        str(row["up_buy_price_chainlink_unit_raw"]),
        row["chainlink_price_unit_scale"],
    )
    with conn.cursor() as cur:
        cur.execute(sql, params)


def parse_args():
    load_dotenv()
    p = argparse.ArgumentParser(description="Fetch Polymarket Up buy price and store into MySQL every second")
    p.add_argument("--symbols", default=SYMBOLS_DEFAULT, help="Comma-separated symbols, default: btc,eth,sol")
    p.add_argument("--once", action="store_true", help="Run one second and exit")
    p.add_argument("--fetch-timeout-seconds", type=float, default=0.8, help="HTTP timeout per request")

    p.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--mysql-database", default=os.getenv("MYSQL_DATABASE", ""))
    p.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", ""))
    p.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", ""))
    p.add_argument("--mysql-table", default=os.getenv("POLYMARKET_TABLE", DEFAULT_TABLE))
    p.add_argument("--mysql-daily-tables", default=os.getenv("MYSQL_DAILY_TABLES", "1"))
    p.add_argument("--mysql-table-date-tz", default=os.getenv("MYSQL_TABLE_DATE_TZ", DEFAULT_TABLE_DATE_TZ))

    args = p.parse_args()
    if not args.mysql_database or not args.mysql_user or not args.mysql_password:
        p.error("MYSQL_DATABASE / MYSQL_USER / MYSQL_PASSWORD are required")
    try:
        args.mysql_daily_tables = parse_bool_flag(args.mysql_daily_tables)
        args.mysql_table_timezone = parse_timezone_offset(args.mysql_table_date_tz)
    except ValueError as exc:
        p.error(str(exc))
    return args


def main():
    args = parse_args()
    symbols = [s.strip().lower() for s in args.symbols.split(",") if s.strip()]
    table_resolver = TableNameResolver(
        daily_enabled=args.mysql_daily_tables,
        table_timezone=args.mysql_table_timezone,
    )

    conn = mysql_connect(args)
    current_table = table_resolver.resolve(args.mysql_table, int(time.time()))
    ensure_table(conn, current_table)
    print(f"table_base: {args.mysql_table}")
    print(f"table_daily_split: {args.mysql_daily_tables}")
    print(f"table_date_tz: {args.mysql_table_date_tz}")
    print(f"active_table: {current_table}")

    state = {
        s: {
            "market_start_ts": None,
            "market_end_ts": None,
            "market_slug": None,
            "up_token_id": None,
            "last_price": None,
        }
        for s in symbols
    }

    # Warm up: ensure each symbol has at least one valid price before strict per-second writes.
    pending = set(symbols)
    while pending:
        now_ts = int(time.time())
        ws = aligned_window_start(now_ts)
        we = ws + 300
        for symbol in list(pending):
            st = state[symbol]
            st["market_start_ts"] = ws
            st["market_end_ts"] = we
            st["market_slug"] = f"{symbol}-updown-5m-{ws}"
            try:
                market = fetch_market_by_slug(st["market_slug"], timeout=args.fetch_timeout_seconds)
                st["up_token_id"] = parse_up_token_id(market)
                st["last_price"] = fetch_up_buy_price(st["up_token_id"], timeout=args.fetch_timeout_seconds)
                pending.remove(symbol)
                print(f"[warmup] {symbol} ready price={st['last_price']}")
            except Exception as exc:  # noqa: BLE001
                print(f"[warmup] {symbol} waiting: {exc}")
        if pending:
            time.sleep(0.5)

    next_tick = int(time.time()) + 1
    executor = ThreadPoolExecutor(max_workers=len(symbols))
    current_minute = None
    minute_inserted = 0
    try:
        while True:
            sleep_for = max(0.0, next_tick - time.time())
            if sleep_for > 0:
                time.sleep(sleep_for)
            ts = next_tick
            next_tick += 1
            now_utc = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            ws = aligned_window_start(ts)
            we = ws + 300

            for symbol in symbols:
                st = state[symbol]
                if st["market_start_ts"] != ws:
                    st["market_start_ts"] = ws
                    st["market_end_ts"] = we
                    st["market_slug"] = f"{symbol}-updown-5m-{ws}"
                    st["up_token_id"] = None

            futures = {}
            for symbol in symbols:
                st = state[symbol]
                if st["up_token_id"] is None:
                    try:
                        market = fetch_market_by_slug(st["market_slug"], timeout=args.fetch_timeout_seconds)
                        st["up_token_id"] = parse_up_token_id(market)
                    except Exception:
                        pass
                if st["up_token_id"] is not None:
                    futures[executor.submit(fetch_up_buy_price, st["up_token_id"], args.fetch_timeout_seconds)] = symbol

            for fut in as_completed(futures):
                symbol = futures[fut]
                try:
                    state[symbol]["last_price"] = fut.result()
                except Exception:
                    pass

            inserted = 0
            for symbol in symbols:
                st = state[symbol]
                if st["last_price"] is None or st["up_token_id"] is None:
                    continue

                row = {
                    "captured_at_ts": ts,
                    "symbol": symbol,
                    "market_slug": st["market_slug"],
                    "market_start_ts": st["market_start_ts"],
                    "market_end_ts": st["market_end_ts"],
                    "up_token_id": st["up_token_id"],
                    "up_buy_price": st["last_price"],
                    "up_buy_price_chainlink_unit_raw": to_chainlink_unit(st["last_price"]),
                    "chainlink_price_unit_scale": 18,
                }
                target_table = table_resolver.resolve(args.mysql_table, ts)
                if target_table != current_table:
                    ensure_table(conn, target_table)
                    current_table = target_table
                    print(f"[table-switch] active_table={current_table}")
                insert_row(conn, current_table, row)
                inserted += 1

            minute_bucket = ts // 60
            if current_minute is None:
                current_minute = minute_bucket
            if minute_bucket != current_minute:
                minute_start_ts = current_minute * 60
                minute_label = datetime.fromtimestamp(minute_start_ts, tz=timezone.utc).astimezone(
                    timezone(timedelta(hours=8))
                ).strftime("%Y-%m-%d %H:%M")
                print(f"[{minute_label} UTC+8] inserted={minute_inserted}")
                current_minute = minute_bucket
                minute_inserted = 0
            minute_inserted += inserted

            if args.once:
                break
    finally:
        if current_minute is not None:
            minute_start_ts = current_minute * 60
            minute_label = datetime.fromtimestamp(minute_start_ts, tz=timezone.utc).astimezone(
                timezone(timedelta(hours=8))
            ).strftime("%Y-%m-%d %H:%M")
            print(f"[{minute_label} UTC+8] inserted={minute_inserted}")
        executor.shutdown(wait=True)
        conn.close()


if __name__ == "__main__":
    main()
