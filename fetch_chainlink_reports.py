#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from table_naming import TableNameResolver, parse_bool_flag, parse_timezone_offset

DEFAULT_TABLE = "chainlink_live_reports"
DEFAULT_WINDOWS_TABLE = "chainlink_5m_windows"
BASE_URL = "https://data.chain.link/api/query-timescale"
QUERY_NAME = "LIVE_STREAM_REPORTS_QUERY"
DEFAULT_INTERVAL_SECONDS = 15
TZ_UTC8 = timezone(timedelta(hours=8))
DEFAULT_TABLE_DATE_TZ = "+08:00"


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if value and ((value[0] == value[-1]) and value[0] in ('"', "'")):
                value = value[1:-1]
            os.environ.setdefault(key, value)


def build_url(feed_id: str) -> str:
    variables = json.dumps({"feedId": feed_id}, separators=(",", ":"))
    params = urlencode({"query": QUERY_NAME, "variables": variables})
    return f"{BASE_URL}?{params}"


def fetch_json(url: str, timeout: int = 20) -> dict:
    req = Request(url, headers={"User-Agent": "poly-valid-price/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        body = resp.read().decode(charset)
    return json.loads(body)


def find_reports(payload):
    if isinstance(payload, dict):
        for key in ("liveStreamReports", "reports", "rows", "result"):
            value = payload.get(key)
            if isinstance(value, list) and (not value or isinstance(value[0], dict)):
                return value
        for value in payload.values():
            found = find_reports(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        if payload and isinstance(payload[0], dict):
            return payload
        for value in payload:
            found = find_reports(value)
            if found is not None:
                return found
    return None


def deep_pick(obj, keys):
    key_set = set(keys)
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in key_set and value is not None:
                return value
        for value in obj.values():
            found = deep_pick(value, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = deep_pick(value, keys)
            if found is not None:
                return found
    return None


def to_unix_timestamp(v):
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if s.isdigit():
            return int(s)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return None
    if isinstance(v, (int, float)):
        vv = float(v)
        if vv >= 1e18:
            vv = vv / 1e9
        elif vv >= 1e15:
            vv = vv / 1e6
        elif vv >= 1e12:
            vv = vv / 1e3
        return int(vv)
    return None


def ts_to_utc8_str(ts_unix):
    if ts_unix is None:
        return "None"
    return datetime.fromtimestamp(ts_unix, tz=TZ_UTC8).strftime("%Y-%m-%d %H:%M:%S")


def five_minute_bucket(ts_unix: int):
    start = (ts_unix // 300) * 300
    end = start + 300
    return start, end


def normalize(report: dict, symbol: str, feed_id: str):
    ts_raw = deep_pick(
        report,
        (
            "validFromTimestamp",
            "updatedAt",
            "timestamp",
            "observationTimestamp",
            "observationsTimestamp",
            "reportedAt",
            "time",
            "createdAt",
        ),
    )
    ts_unix = to_unix_timestamp(ts_raw)
    if ts_unix is None:
        return None

    start_at, end_at = five_minute_bucket(ts_unix)
    return {
        "symbol": symbol,
        "feed_id": feed_id,
        "ts_unix": ts_unix,
        "start_at": start_at,
        "end_at": end_at,
        "price": deep_pick(report, ("price",)),
        "bid": deep_pick(report, ("bid",)),
        "ask": deep_pick(report, ("ask",)),
    }


def ensure_tables(conn, reports_table: str, windows_table: str):
    reports_sql = f"""
    CREATE TABLE IF NOT EXISTS `{reports_table}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      symbol VARCHAR(16) NOT NULL,
      feed_id VARCHAR(128) NOT NULL,
      ts_unix BIGINT NOT NULL,
      start_at BIGINT NOT NULL,
      end_at BIGINT NOT NULL,
      price VARCHAR(128) NULL,
      bid VARCHAR(128) NULL,
      ask VARCHAR(128) NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_symbol_ts (symbol, ts_unix),
      KEY idx_symbol_window (symbol, start_at, end_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    windows_sql = f"""
    CREATE TABLE IF NOT EXISTS `{windows_table}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      symbol VARCHAR(16) NOT NULL,
      start_at BIGINT NOT NULL,
      end_at BIGINT NOT NULL,
      row_count INT NOT NULL,
      expected_count INT NOT NULL DEFAULT 300,
      is_complete TINYINT(1) NOT NULL DEFAULT 0,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_symbol_window (symbol, start_at, end_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    with conn.cursor() as cur:
        cur.execute(reports_sql)
        cur.execute(windows_sql)


def insert_reports(conn, reports_table: str, rows):
    if not rows:
        return 0

    sql = f"""
    INSERT IGNORE INTO `{reports_table}` (
      symbol, feed_id, ts_unix, start_at, end_at, price, bid, ask
    ) VALUES (
      %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    values = [
        (
            r["symbol"],
            r["feed_id"],
            r["ts_unix"],
            r["start_at"],
            r["end_at"],
            r["price"],
            r["bid"],
            r["ask"],
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        cur.executemany(sql, values)
        return cur.rowcount


def insert_reports_grouped(conn, reports_base_table: str, windows_base_table: str, rows, table_resolver):
    if not rows:
        return 0, set()

    grouped_rows = {}
    touched_table_pairs = set()
    for row in rows:
        reports_table = table_resolver.resolve(reports_base_table, row["ts_unix"])
        windows_table = table_resolver.resolve(windows_base_table, row["start_at"])
        grouped_rows.setdefault((reports_table, windows_table), []).append(row)
        touched_table_pairs.add((reports_table, windows_table))

    total_inserted = 0
    for reports_table, windows_table in grouped_rows:
        ensure_tables(conn, reports_table, windows_table)
        total_inserted += insert_reports(conn, reports_table, grouped_rows[(reports_table, windows_table)])
    return total_inserted, touched_table_pairs


def refresh_windows(conn, reports_table: str, windows_table: str):
    sql = f"""
    INSERT INTO `{windows_table}` (
      symbol, start_at, end_at, row_count, expected_count, is_complete
    )
    SELECT
      symbol,
      start_at,
      end_at,
      COUNT(*) AS row_count,
      300 AS expected_count,
      CASE WHEN COUNT(*) >= 300 THEN 1 ELSE 0 END AS is_complete
    FROM `{reports_table}`
    GROUP BY symbol, start_at, end_at
    ON DUPLICATE KEY UPDATE
      row_count = VALUES(row_count),
      expected_count = VALUES(expected_count),
      is_complete = VALUES(is_complete),
      updated_at = CURRENT_TIMESTAMP
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def fetch_incomplete_windows(conn, windows_table: str):
    sql = f"""
    SELECT symbol, start_at, end_at, row_count, expected_count,
           (expected_count - row_count) AS missing
    FROM `{windows_table}`
    WHERE end_at <= UNIX_TIMESTAMP()
      AND is_complete = 0
    ORDER BY end_at DESC
    LIMIT 12
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def resolve_feeds(args):
    feeds = {}
    env_default = {
        "ETH": os.getenv("CHAINLINK_FEED_ETH", "").strip(),
        "BTC": os.getenv("CHAINLINK_FEED_BTC", "").strip(),
        "SOL": os.getenv("CHAINLINK_FEED_SOL", "").strip(),
    }
    for symbol, fid in env_default.items():
        if fid:
            feeds[symbol] = fid

    for item in args.feed:
        if ":" not in item:
            raise ValueError(f"Invalid --feed format: {item}. Use SYMBOL:FEED_ID")
        symbol, fid = item.split(":", 1)
        symbol = symbol.strip().upper()
        fid = fid.strip()
        if not symbol or not fid:
            raise ValueError(f"Invalid --feed value: {item}")
        feeds[symbol] = fid

    required = ["ETH", "BTC", "SOL"]
    missing = [sym for sym in required if sym not in feeds]
    if missing:
        raise ValueError(
            "Missing feed ids for: " + ", ".join(missing) + ". "
            "Set CHAINLINK_FEED_ETH/BTC/SOL in .env or pass --feed SYMBOL:FEED_ID"
        )
    return feeds


def connect_mysql(args):
    try:
        import pymysql
    except ImportError:
        print("Missing dependency: pymysql. Install with: pip install pymysql", file=sys.stderr)
        sys.exit(1)

    return pymysql.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        charset="utf8mb4",
        autocommit=False,
    )


def run_cycle(conn, feeds, args, table_resolver):
    total_inserted = 0
    total_seen = 0
    now_utc8 = datetime.now(TZ_UTC8).strftime("%Y-%m-%d %H:%M:%S")
    cycle_touched_table_pairs = set()

    for symbol, feed_id in feeds.items():
        url = build_url(feed_id)
        try:
            payload = fetch_json(url)
        except (HTTPError, URLError, json.JSONDecodeError) as e:
            print(f"[{symbol}] fetch_failed: {e}", file=sys.stderr)
            continue

        raw_reports = find_reports(payload) or []
        parsed_rows = []
        for report in raw_reports:
            row = normalize(report, symbol=symbol, feed_id=feed_id)
            if row is not None:
                parsed_rows.append(row)

        total_seen += len(parsed_rows)
        inserted, touched_table_pairs = insert_reports_grouped(
            conn,
            args.mysql_table,
            args.mysql_windows_table,
            parsed_rows,
            table_resolver,
        )
        total_inserted += inserted
        cycle_touched_table_pairs.update(touched_table_pairs)
        latest_ts = parsed_rows[0]["ts_unix"] if parsed_rows else None
        print(
            f"[{now_utc8} UTC+8] [{symbol}] fetched={len(parsed_rows)} inserted={inserted} "
            f"latest_ts={latest_ts} latest_utc8={ts_to_utc8_str(latest_ts)}"
        )
    for reports_table, windows_table in cycle_touched_table_pairs:
        refresh_windows(conn, reports_table, windows_table)
    conn.commit()
    print(f"[{now_utc8} UTC+8] cycle_summary: seen={total_seen} inserted={total_inserted}")


def parse_args():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Continuously ingest Chainlink reports into MySQL with 5-minute windows"
    )
    parser.add_argument("--feed", action="append", default=[], help="Override feed: SYMBOL:FEED_ID")
    parser.add_argument("--interval-seconds", type=int, default=DEFAULT_INTERVAL_SECONDS, help="Polling interval")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")

    parser.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--mysql-database", default=os.getenv("MYSQL_DATABASE", ""))
    parser.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", ""))
    parser.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", ""))
    parser.add_argument("--mysql-table", default=os.getenv("MYSQL_TABLE", DEFAULT_TABLE))
    parser.add_argument("--mysql-windows-table", default=os.getenv("MYSQL_WINDOWS_TABLE", DEFAULT_WINDOWS_TABLE))
    parser.add_argument("--mysql-daily-tables", default=os.getenv("MYSQL_DAILY_TABLES", "1"))
    parser.add_argument("--mysql-table-date-tz", default=os.getenv("MYSQL_TABLE_DATE_TZ", DEFAULT_TABLE_DATE_TZ))

    args = parser.parse_args()
    if not args.mysql_database or not args.mysql_user:
        parser.error("MYSQL_DATABASE and MYSQL_USER are required (set in .env or CLI args)")
    try:
        args.mysql_daily_tables = parse_bool_flag(args.mysql_daily_tables)
        args.mysql_table_timezone = parse_timezone_offset(args.mysql_table_date_tz)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main():
    args = parse_args()
    try:
        feeds = resolve_feeds(args)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    table_resolver = TableNameResolver(
        daily_enabled=args.mysql_daily_tables,
        table_timezone=args.mysql_table_timezone,
    )
    conn = connect_mysql(args)
    try:
        now_ts = int(time.time())
        active_reports_table = table_resolver.resolve(args.mysql_table, now_ts)
        active_windows_table = table_resolver.resolve(args.mysql_windows_table, now_ts)
        ensure_tables(conn, active_reports_table, active_windows_table)
        conn.commit()

        print("feeds:")
        for symbol, feed_id in feeds.items():
            print(f"  {symbol}: {feed_id}")
        print(f"interval_seconds: {args.interval_seconds}")
        print(f"reports_table_base: {args.mysql_table}")
        print(f"windows_table_base: {args.mysql_windows_table}")
        print(f"table_daily_split: {args.mysql_daily_tables}")
        print(f"table_date_tz: {args.mysql_table_date_tz}")
        print(f"active_reports_table: {active_reports_table}")
        print(f"active_windows_table: {active_windows_table}")

        if args.once:
            run_cycle(conn, feeds, args, table_resolver)
            return

        while True:
            started = time.time()
            run_cycle(conn, feeds, args, table_resolver)
            elapsed = time.time() - started
            sleep_for = max(0.0, args.interval_seconds - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
