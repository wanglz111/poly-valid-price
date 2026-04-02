#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import queue
import signal
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pymysql
import websocket

from table_naming import TableNameResolver, parse_bool_flag, parse_timezone_offset

GAMMA_BASE = "https://gamma-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_TABLE = "polymarket_wss_events"
SYMBOLS_DEFAULT = "btc,eth,sol"
DEFAULT_TABLE_DATE_TZ = "+08:00"
HEARTBEAT_INTERVAL_SECONDS = 10.0


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
            if value and value[0] == value[-1] and value[0] in ('"', "'"):
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


def fetch_market_by_slug(slug: str, timeout: float):
    return http_json(f"{GAMMA_BASE}/markets/slug/{urllib.parse.quote(slug)}", timeout=timeout)


def parse_json_maybe(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


def parse_up_token_id(market) -> str:
    outcomes_raw = market.get("outcomes")
    token_ids_raw = market.get("clobTokenIds") or market.get("clob_token_ids")
    if not outcomes_raw or not token_ids_raw:
        raise ValueError("market payload missing outcomes/clobTokenIds")

    outcomes = parse_json_maybe(outcomes_raw)
    token_ids = parse_json_maybe(token_ids_raw)

    for idx, outcome in enumerate(outcomes):
        if str(outcome).lower() == "up":
            return str(token_ids[idx])
    raise ValueError("cannot find Up outcome")


def parse_condition_id(market) -> str:
    for key in ("conditionId", "condition_id", "market", "condition"):
        value = market.get(key)
        if value:
            return str(value)
    raise ValueError("market payload missing condition id")


def parse_decimal_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(Decimal(str(value)))


def parse_int_maybe(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(str(value))


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
      captured_at_ms BIGINT NOT NULL,
      event_hash CHAR(64) NOT NULL,
      event_type VARCHAR(32) NOT NULL,
      symbol VARCHAR(16) NOT NULL,
      market_slug VARCHAR(128) NOT NULL,
      market_condition_id VARCHAR(128) NOT NULL,
      market_start_ts BIGINT NOT NULL,
      market_end_ts BIGINT NOT NULL,
      asset_id VARCHAR(128) NOT NULL,
      event_timestamp_ms BIGINT NULL,
      best_bid DECIMAL(30,18) NULL,
      best_ask DECIMAL(30,18) NULL,
      spread DECIMAL(30,18) NULL,
      last_trade_price DECIMAL(30,18) NULL,
      last_trade_side VARCHAR(8) NULL,
      last_trade_size DECIMAL(30,18) NULL,
      last_trade_fee_rate_bps DECIMAL(20,8) NULL,
      transaction_hash VARCHAR(128) NULL,
      payload_json LONGTEXT NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uq_event_hash (event_hash),
      KEY idx_symbol_event_ts (symbol, event_type, event_timestamp_ms),
      KEY idx_asset_event_ts (asset_id, event_type, event_timestamp_ms),
      KEY idx_market_window (market_slug, market_start_ts, market_end_ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def insert_rows(conn, table: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    sql = f"""
    INSERT IGNORE INTO `{table}` (
      captured_at_ts,
      captured_at_ms,
      event_hash,
      event_type,
      symbol,
      market_slug,
      market_condition_id,
      market_start_ts,
      market_end_ts,
      asset_id,
      event_timestamp_ms,
      best_bid,
      best_ask,
      spread,
      last_trade_price,
      last_trade_side,
      last_trade_size,
      last_trade_fee_rate_bps,
      transaction_hash,
      payload_json
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = [
        (
            row["captured_at_ts"],
            row["captured_at_ms"],
            row["event_hash"],
            row["event_type"],
            row["symbol"],
            row["market_slug"],
            row["market_condition_id"],
            row["market_start_ts"],
            row["market_end_ts"],
            row["asset_id"],
            row["event_timestamp_ms"],
            row["best_bid"],
            row["best_ask"],
            row["spread"],
            row["last_trade_price"],
            row["last_trade_side"],
            row["last_trade_size"],
            row["last_trade_fee_rate_bps"],
            row["transaction_hash"],
            row["payload_json"],
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        affected = cur.executemany(sql, params)
    return int(affected or 0)


@dataclass(frozen=True)
class MarketBinding:
    symbol: str
    market_slug: str
    market_condition_id: str
    market_start_ts: int
    market_end_ts: int
    asset_id: str


class BatchWriter(threading.Thread):
    def __init__(self, args, row_queue: queue.Queue, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.args = args
        self.row_queue = row_queue
        self.stop_event = stop_event
        self.table_resolver = TableNameResolver(
            daily_enabled=args.mysql_daily_tables,
            table_timezone=args.mysql_table_timezone,
        )
        self.current_table = None

    def _ensure_target_table(self, conn, ts_unix: int) -> str:
        target_table = self.table_resolver.resolve(self.args.mysql_table, ts_unix)
        if target_table != self.current_table:
            ensure_table(conn, target_table)
            self.current_table = target_table
            print(f"[writer] active_table={self.current_table}")
        return self.current_table

    def run(self) -> None:
        conn = mysql_connect(self.args)
        batch: list[dict] = []
        last_flush = time.monotonic()
        try:
            while not self.stop_event.is_set() or not self.row_queue.empty() or batch:
                timeout = max(
                    0.0,
                    self.args.flush_interval_seconds - (time.monotonic() - last_flush),
                )
                try:
                    row = self.row_queue.get(timeout=timeout if batch else 0.5)
                    batch.append(row)
                except queue.Empty:
                    pass

                while len(batch) < self.args.batch_size:
                    try:
                        batch.append(self.row_queue.get_nowait())
                    except queue.Empty:
                        break

                should_flush = False
                if batch and len(batch) >= self.args.batch_size:
                    should_flush = True
                if batch and time.monotonic() - last_flush >= self.args.flush_interval_seconds:
                    should_flush = True
                if batch and self.stop_event.is_set() and self.row_queue.empty():
                    should_flush = True

                if not should_flush:
                    continue

                table = self._ensure_target_table(conn, batch[-1]["captured_at_ts"])
                inserted = insert_rows(conn, table, batch)
                print(f"[writer] flushed={len(batch)} inserted={inserted}")
                batch.clear()
                last_flush = time.monotonic()
        finally:
            conn.close()


class PolymarketWsCollector:
    def __init__(self, args):
        self.args = args
        self.symbols = [s.strip().lower() for s in args.symbols.split(",") if s.strip()]
        self.row_queue: queue.Queue = queue.Queue(maxsize=args.queue_maxsize)
        self.stop_event = threading.Event()
        self.state_lock = threading.Lock()
        self.ws_send_lock = threading.Lock()
        self.ws_app: websocket.WebSocketApp | None = None
        self.active_bindings: dict[str, MarketBinding] = {}
        self.known_assets: dict[str, MarketBinding] = {}
        self.last_discovery_error: dict[str, str] = {}
        self.dropped_events = 0
        self.writer = BatchWriter(args, self.row_queue, self.stop_event)

    def start(self) -> None:
        self.writer.start()

        discovery_thread = threading.Thread(target=self.discovery_loop, daemon=True)
        heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        discovery_thread.start()
        heartbeat_thread.start()

        if not self.wait_for_initial_assets():
            raise RuntimeError("no active asset ids discovered before stop")

        try:
            self.ws_loop()
        finally:
            self.stop_event.set()
            self.writer.join()

    def stop(self) -> None:
        self.stop_event.set()
        with self.ws_send_lock:
            app = self.ws_app
        if app is not None:
            try:
                app.close()
            except Exception:
                pass

    def wait_for_initial_assets(self) -> bool:
        while not self.stop_event.is_set():
            if self.get_asset_ids():
                return True
            print("[gamma] waiting for initial active 5m market tokens")
            time.sleep(1.0)
        return False

    def discovery_loop(self) -> None:
        while not self.stop_event.is_set():
            self.refresh_current_bindings()
            time.sleep(self.args.refresh_interval_seconds)

    def heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)
            if self.stop_event.is_set():
                break
            with self.ws_send_lock:
                app = self.ws_app
            if app is None or not app.sock or not app.sock.connected:
                continue
            try:
                app.send(json.dumps({}))
            except Exception as exc:  # noqa: BLE001
                print(f"[ws] heartbeat failed: {exc}")

    def ws_loop(self) -> None:
        while not self.stop_event.is_set():
            app = websocket.WebSocketApp(
                self.args.ws_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            with self.ws_send_lock:
                self.ws_app = app
            print(f"[ws] connecting url={self.args.ws_url}")
            app.run_forever(ping_interval=20, ping_timeout=10, ping_payload="ping")
            with self.ws_send_lock:
                if self.ws_app is app:
                    self.ws_app = None
            if self.stop_event.is_set():
                break
            print("[ws] disconnected, retry in 2s")
            time.sleep(2.0)

    def on_open(self, ws):
        asset_ids = self.get_asset_ids()
        if not asset_ids:
            print("[ws] opened but no asset ids ready yet")
            return
        payload = {
            "assets_ids": asset_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        ws.send(json.dumps(payload))
        print(f"[ws] subscribed asset_count={len(asset_ids)} assets={','.join(asset_ids)}")

    def on_message(self, _ws, message: str):
        if message in {"PONG", "PING"}:
            return
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        self.handle_payload(payload)

    def on_error(self, _ws, error):
        print(f"[ws] error: {error}")

    def on_close(self, _ws, status_code, message):
        print(f"[ws] closed status={status_code} message={message}")

    def handle_payload(self, payload):
        if isinstance(payload, list):
            for item in payload:
                self.handle_payload(item)
            return
        if not isinstance(payload, dict):
            return

        event_type = str(payload.get("event_type") or "").strip()
        if event_type not in {"best_bid_ask", "last_trade_price"}:
            return

        asset_id = str(payload.get("asset_id") or "")
        binding = self.lookup_binding(asset_id)
        if binding is None:
            print(f"[ws] skip event without binding event_type={event_type} asset_id={asset_id}")
            return

        raw_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        captured_at_ms = int(time.time() * 1000)
        row = {
            "captured_at_ts": captured_at_ms // 1000,
            "captured_at_ms": captured_at_ms,
            "event_hash": hashlib.sha256(raw_payload.encode("utf-8")).hexdigest(),
            "event_type": event_type,
            "symbol": binding.symbol,
            "market_slug": binding.market_slug,
            "market_condition_id": str(payload.get("market") or binding.market_condition_id),
            "market_start_ts": binding.market_start_ts,
            "market_end_ts": binding.market_end_ts,
            "asset_id": asset_id,
            "event_timestamp_ms": parse_int_maybe(payload.get("timestamp")),
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "last_trade_price": None,
            "last_trade_side": None,
            "last_trade_size": None,
            "last_trade_fee_rate_bps": None,
            "transaction_hash": str(payload.get("transaction_hash") or "") or None,
            "payload_json": raw_payload,
        }

        if event_type == "best_bid_ask":
            row["best_bid"] = parse_decimal_str(payload.get("best_bid"))
            row["best_ask"] = parse_decimal_str(payload.get("best_ask"))
            row["spread"] = parse_decimal_str(payload.get("spread"))
        elif event_type == "last_trade_price":
            row["last_trade_price"] = parse_decimal_str(payload.get("price"))
            row["last_trade_side"] = str(payload.get("side") or "") or None
            row["last_trade_size"] = parse_decimal_str(payload.get("size"))
            row["last_trade_fee_rate_bps"] = parse_decimal_str(payload.get("fee_rate_bps"))

        try:
            self.row_queue.put_nowait(row)
        except queue.Full:
            self.dropped_events += 1
            if self.dropped_events == 1 or self.dropped_events % 100 == 0:
                print(f"[writer] queue full dropped_events={self.dropped_events}")

    def refresh_current_bindings(self) -> None:
        now_ts = int(time.time())
        market_start_ts = aligned_window_start(now_ts)
        market_end_ts = market_start_ts + 300

        with self.state_lock:
            existing = dict(self.active_bindings)

        desired_bindings: dict[str, MarketBinding] = {}
        for symbol in self.symbols:
            slug = f"{symbol}-updown-5m-{market_start_ts}"
            current = existing.get(symbol)
            if current and current.market_start_ts == market_start_ts:
                desired_bindings[symbol] = current
                continue

            try:
                market = fetch_market_by_slug(slug, timeout=self.args.fetch_timeout_seconds)
                binding = MarketBinding(
                    symbol=symbol,
                    market_slug=slug,
                    market_condition_id=parse_condition_id(market),
                    market_start_ts=market_start_ts,
                    market_end_ts=market_end_ts,
                    asset_id=parse_up_token_id(market),
                )
                desired_bindings[symbol] = binding
                self.last_discovery_error.pop(symbol, None)
            except Exception as exc:  # noqa: BLE001
                error_text = f"{type(exc).__name__}: {exc}"
                if self.last_discovery_error.get(symbol) != error_text:
                    print(f"[gamma] {symbol} slug={slug} unavailable: {error_text}")
                    self.last_discovery_error[symbol] = error_text

        with self.state_lock:
            old_asset_ids = {binding.asset_id for binding in self.active_bindings.values()}
            self.active_bindings = desired_bindings
            for binding in desired_bindings.values():
                self.known_assets[binding.asset_id] = binding
            new_asset_ids = {binding.asset_id for binding in desired_bindings.values()}

        if old_asset_ids != new_asset_ids:
            removed = sorted(old_asset_ids - new_asset_ids)
            added = sorted(new_asset_ids - old_asset_ids)
            print(
                "[gamma] active bindings "
                f"symbols={len(desired_bindings)}/{len(self.symbols)} "
                f"added={len(added)} removed={len(removed)}"
            )
            if removed:
                self.send_subscription_update("unsubscribe", removed)
            if added:
                self.send_subscription_update("subscribe", added)

    def get_asset_ids(self) -> list[str]:
        with self.state_lock:
            return sorted(binding.asset_id for binding in self.active_bindings.values())

    def lookup_binding(self, asset_id: str) -> MarketBinding | None:
        with self.state_lock:
            return self.known_assets.get(asset_id)

    def send_subscription_update(self, operation: str, asset_ids: list[str]) -> None:
        if not asset_ids:
            return
        with self.ws_send_lock:
            app = self.ws_app
        if app is None or not app.sock or not app.sock.connected:
            return
        payload = {"operation": operation, "assets_ids": asset_ids}
        if operation == "subscribe":
            payload["custom_feature_enabled"] = True
        try:
            app.send(json.dumps(payload))
            print(f"[ws] {operation} asset_count={len(asset_ids)} assets={','.join(asset_ids)}")
        except Exception as exc:  # noqa: BLE001
            print(f"[ws] {operation} failed: {exc}")


def parse_args():
    load_dotenv()
    p = argparse.ArgumentParser(
        description="Listen to Polymarket market websocket for current 5m Up tokens and batch write events into MySQL"
    )
    p.add_argument("--symbols", default=SYMBOLS_DEFAULT, help="Comma-separated symbols, default: btc,eth,sol")
    p.add_argument("--fetch-timeout-seconds", type=float, default=0.8, help="HTTP timeout per Gamma request")
    p.add_argument(
        "--refresh-interval-seconds",
        type=float,
        default=0.5,
        help="Refresh current 5m market bindings at this interval",
    )
    p.add_argument(
        "--flush-interval-seconds",
        type=float,
        default=1.0,
        help="Flush pending websocket events to MySQL at this interval",
    )
    p.add_argument("--batch-size", type=int, default=100, help="Max rows per MySQL batch insert")
    p.add_argument("--queue-maxsize", type=int, default=20000, help="In-memory event queue size")
    p.add_argument("--ws-url", default=WS_MARKET_URL, help=f"Polymarket market websocket URL, default: {WS_MARKET_URL}")

    p.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--mysql-database", default=os.getenv("MYSQL_DATABASE", ""))
    p.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", ""))
    p.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", ""))
    p.add_argument("--mysql-table", default=os.getenv("POLYMARKET_WSS_TABLE", DEFAULT_TABLE))
    p.add_argument("--mysql-daily-tables", default=os.getenv("MYSQL_DAILY_TABLES", "1"))
    p.add_argument("--mysql-table-date-tz", default=os.getenv("MYSQL_TABLE_DATE_TZ", DEFAULT_TABLE_DATE_TZ))

    args = p.parse_args()
    if not args.mysql_database or not args.mysql_user or not args.mysql_password:
        p.error("MYSQL_DATABASE / MYSQL_USER / MYSQL_PASSWORD are required")
    if args.batch_size <= 0:
        p.error("--batch-size must be > 0")
    if args.flush_interval_seconds <= 0:
        p.error("--flush-interval-seconds must be > 0")
    if args.refresh_interval_seconds <= 0:
        p.error("--refresh-interval-seconds must be > 0")
    if args.queue_maxsize <= 0:
        p.error("--queue-maxsize must be > 0")
    try:
        args.mysql_daily_tables = parse_bool_flag(args.mysql_daily_tables)
        args.mysql_table_timezone = parse_timezone_offset(args.mysql_table_date_tz)
    except ValueError as exc:
        p.error(str(exc))
    return args


def main():
    args = parse_args()
    collector = PolymarketWsCollector(args)

    def handle_stop(_signum, _frame):
        print("[main] stop signal received")
        collector.stop()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    print(f"symbols: {args.symbols}")
    print(f"mysql_table_base: {args.mysql_table}")
    print(f"mysql_daily_tables: {args.mysql_daily_tables}")
    print(f"mysql_table_date_tz: {args.mysql_table_date_tz}")
    print(f"ws_url: {args.ws_url}")
    collector.start()


if __name__ == "__main__":
    main()
