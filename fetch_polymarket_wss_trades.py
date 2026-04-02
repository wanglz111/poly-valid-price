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
DEFAULT_TABLE = "polymarket_wss_trades"
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
      symbol VARCHAR(16) NOT NULL,
      market_slug VARCHAR(128) NOT NULL,
      market_condition_id VARCHAR(128) NOT NULL,
      market_start_ts BIGINT NOT NULL,
      market_end_ts BIGINT NOT NULL,
      outcome VARCHAR(16) NOT NULL,
      asset_id VARCHAR(128) NOT NULL,
      event_timestamp_ms BIGINT NULL,
      trade_price DECIMAL(30,18) NOT NULL,
      trade_side VARCHAR(8) NULL,
      trade_size DECIMAL(30,18) NULL,
      trade_fee_rate_bps DECIMAL(20,8) NULL,
      transaction_hash VARCHAR(128) NULL,
      payload_json LONGTEXT NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uq_event_hash (event_hash),
      KEY idx_symbol_trade_ts (symbol, outcome, event_timestamp_ms),
      KEY idx_asset_trade_ts (asset_id, event_timestamp_ms),
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
      symbol,
      market_slug,
      market_condition_id,
      market_start_ts,
      market_end_ts,
      outcome,
      asset_id,
      event_timestamp_ms,
      trade_price,
      trade_side,
      trade_size,
      trade_fee_rate_bps,
      transaction_hash,
      payload_json
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = [
        (
            row["captured_at_ts"],
            row["captured_at_ms"],
            row["event_hash"],
            row["symbol"],
            row["market_slug"],
            row["market_condition_id"],
            row["market_start_ts"],
            row["market_end_ts"],
            row["outcome"],
            row["asset_id"],
            row["event_timestamp_ms"],
            row["trade_price"],
            row["trade_side"],
            row["trade_size"],
            row["trade_fee_rate_bps"],
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
    outcome: str
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
                insert_rows(conn, table, batch)
                batch.clear()
                last_flush = time.monotonic()
        finally:
            conn.close()


class PolymarketWsTradeCollector:
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
        self.stats_lock = threading.Lock()
        self.window_event_counts: dict[int, dict[tuple[str, str], int]] = {}
        self.window_drop_counts: dict[int, int] = {}
        self.current_window_start_ts: int | None = None
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
        }
        ws.send(json.dumps(payload))
        print(f"[ws] subscribed asset_count={len(asset_ids)}")

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
        if event_type != "last_trade_price":
            return

        asset_id = str(payload.get("asset_id") or "")
        binding = self.lookup_binding(asset_id)
        if binding is None:
            print(f"[ws] skip trade without binding asset_id={asset_id}")
            return

        raw_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        captured_at_ms = int(time.time() * 1000)
        row = {
            "captured_at_ts": captured_at_ms // 1000,
            "captured_at_ms": captured_at_ms,
            "event_hash": hashlib.sha256(raw_payload.encode("utf-8")).hexdigest(),
            "symbol": binding.symbol,
            "market_slug": binding.market_slug,
            "market_condition_id": str(payload.get("market") or binding.market_condition_id),
            "market_start_ts": binding.market_start_ts,
            "market_end_ts": binding.market_end_ts,
            "outcome": binding.outcome,
            "asset_id": asset_id,
            "event_timestamp_ms": parse_int_maybe(payload.get("timestamp")),
            "trade_price": parse_decimal_str(payload.get("price")),
            "trade_side": str(payload.get("side") or "") or None,
            "trade_size": parse_decimal_str(payload.get("size")),
            "trade_fee_rate_bps": parse_decimal_str(payload.get("fee_rate_bps")),
            "transaction_hash": str(payload.get("transaction_hash") or "") or None,
            "payload_json": raw_payload,
        }

        try:
            self.row_queue.put_nowait(row)
            self.record_window_event(binding.market_start_ts, binding.symbol, binding.outcome)
        except queue.Full:
            self.dropped_events += 1
            self.record_window_drop(binding.market_start_ts)
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
            current_bindings = [b for b in existing.values() if b.symbol == symbol and b.market_start_ts == market_start_ts]
            if len(current_bindings) == 2:
                for binding in current_bindings:
                    desired_bindings[binding.asset_id] = binding
                continue

            try:
                market = fetch_market_by_slug(slug, timeout=self.args.fetch_timeout_seconds)
                condition_id = parse_condition_id(market)
                outcomes = parse_json_maybe(market.get("outcomes"))
                token_ids = parse_json_maybe(market.get("clobTokenIds") or market.get("clob_token_ids"))
                if not outcomes or not token_ids or len(outcomes) != len(token_ids):
                    raise ValueError("market payload missing aligned outcomes/clobTokenIds")
                for outcome, token_id in zip(outcomes, token_ids):
                    normalized_outcome = str(outcome).strip()
                    if normalized_outcome.lower() not in {"up", "down"}:
                        continue
                    binding = MarketBinding(
                        symbol=symbol,
                        market_slug=slug,
                        market_condition_id=condition_id,
                        market_start_ts=market_start_ts,
                        market_end_ts=market_end_ts,
                        outcome=normalized_outcome,
                        asset_id=str(token_id),
                    )
                    desired_bindings[binding.asset_id] = binding
                if not any(b.symbol == symbol for b in desired_bindings.values()):
                    raise ValueError("cannot find Up/Down outcomes")
                self.last_discovery_error.pop(symbol, None)
            except Exception as exc:  # noqa: BLE001
                error_text = f"{type(exc).__name__}: {exc}"
                if self.last_discovery_error.get(symbol) != error_text:
                    print(f"[gamma] {symbol} slug={slug} unavailable: {error_text}")
                    self.last_discovery_error[symbol] = error_text

        self.log_window_rollover(market_start_ts, existing, desired_bindings)

        with self.state_lock:
            old_asset_ids = set(self.active_bindings.keys())
            self.active_bindings = desired_bindings
            for asset_id, binding in desired_bindings.items():
                self.known_assets[asset_id] = binding
            new_asset_ids = set(desired_bindings.keys())

        if old_asset_ids != new_asset_ids:
            removed = sorted(old_asset_ids - new_asset_ids)
            added = sorted(new_asset_ids - old_asset_ids)
            print(
                "[gamma] active trade bindings "
                f"assets={len(new_asset_ids)} symbols={len(self.symbols)} "
                f"added={len(added)} removed={len(removed)}"
            )
            if removed:
                self.send_subscription_update("unsubscribe", removed)
            if added:
                self.send_subscription_update("subscribe", added)

    def get_asset_ids(self) -> list[str]:
        with self.state_lock:
            return sorted(self.active_bindings.keys())

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
        try:
            app.send(json.dumps(payload))
            print(f"[ws] {operation} asset_count={len(asset_ids)}")
        except Exception as exc:  # noqa: BLE001
            print(f"[ws] {operation} failed: {exc}")

    def record_window_event(self, window_start_ts: int, symbol: str, outcome: str) -> None:
        with self.stats_lock:
            counts = self.window_event_counts.setdefault(window_start_ts, {})
            key = (symbol, outcome)
            counts[key] = counts.get(key, 0) + 1

    def record_window_drop(self, window_start_ts: int) -> None:
        with self.stats_lock:
            self.window_drop_counts[window_start_ts] = self.window_drop_counts.get(window_start_ts, 0) + 1

    def log_window_rollover(
        self,
        window_start_ts: int,
        previous_bindings: dict[str, MarketBinding],
        current_bindings: dict[str, MarketBinding],
    ) -> None:
        previous_window_start_ts = self.current_window_start_ts
        if previous_window_start_ts is None:
            self.current_window_start_ts = window_start_ts
            print(f"[window] current={self.format_market_list(current_bindings)}")
            return
        if previous_window_start_ts == window_start_ts:
            return

        with self.stats_lock:
            previous_counts = self.window_event_counts.pop(previous_window_start_ts, {})
            previous_drops = self.window_drop_counts.pop(previous_window_start_ts, 0)

        previous_detail = self.format_window_stats(previous_bindings, previous_counts)
        previous_total = sum(previous_counts.values())
        current_detail = self.format_market_list(current_bindings)
        print("[window]")
        print(f"  current={current_detail}")
        print(f"  previous_window_start={previous_window_start_ts}")
        print(f"  previous_valid={previous_total}")
        for line in previous_detail:
            print(f"  {line}")
        print(f"  previous_dropped={previous_drops}")
        self.current_window_start_ts = window_start_ts

    def format_market_list(self, bindings: dict[str, MarketBinding]) -> str:
        market_by_symbol = {}
        for binding in bindings.values():
            market_by_symbol[binding.symbol] = binding.market_slug
        if not market_by_symbol:
            return "none"
        return ",".join(f"{symbol}:{market_by_symbol[symbol]}" for symbol in sorted(market_by_symbol))

    def format_window_stats(
        self,
        bindings: dict[str, MarketBinding],
        counts: dict[tuple[str, str], int],
    ) -> list[str]:
        symbols = sorted({binding.symbol for binding in bindings.values()})
        if not symbols:
            symbols = sorted({symbol for symbol, _outcome in counts})
        if not symbols:
            return ["previous_detail=none"]

        parts = []
        for symbol in symbols:
            up_count = counts.get((symbol, "Up"), 0)
            down_count = counts.get((symbol, "Down"), 0)
            total = up_count + down_count
            parts.append(f"{symbol}: total={total} up={up_count} down={down_count}")
        return parts


def parse_args():
    load_dotenv()
    p = argparse.ArgumentParser(
        description="Listen to Polymarket last_trade_price events for current 5m Up/Down tokens and batch write trades into MySQL"
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
        help="Flush pending websocket trades to MySQL at this interval",
    )
    p.add_argument("--batch-size", type=int, default=100, help="Max rows per MySQL batch insert")
    p.add_argument("--queue-maxsize", type=int, default=20000, help="In-memory trade queue size")
    p.add_argument("--ws-url", default=WS_MARKET_URL, help=f"Polymarket market websocket URL, default: {WS_MARKET_URL}")

    p.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--mysql-database", default=os.getenv("MYSQL_DATABASE", ""))
    p.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", ""))
    p.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", ""))
    p.add_argument("--mysql-table", default=os.getenv("POLYMARKET_WSS_TRADES_TABLE", DEFAULT_TABLE))
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
    collector = PolymarketWsTradeCollector(args)

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
