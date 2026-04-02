#!/usr/bin/env python3
import argparse
import json
import os
import signal
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pymysql
import websocket

GAMMA_BASE = "https://gamma-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SYMBOLS_DEFAULT = "doge,bnb,hype,eth,btc,sol"
HEARTBEAT_INTERVAL_SECONDS = 10.0
DEFAULT_TABLE = "polymarket_trade_volume_windows"


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


def parse_decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def format_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f") if value != 0 else "0"


def format_window_label(window_start_ts: int) -> str:
    start_dt = datetime.fromtimestamp(window_start_ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(window_start_ts + 300, tz=timezone.utc)
    return f"{start_dt:%Y-%m-%d %H:%M:%S}Z -> {end_dt:%H:%M:%S}Z"


def ensure_table(conn, table: str):
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      window_start_ts BIGINT NOT NULL,
      window_end_ts BIGINT NOT NULL,
      symbol VARCHAR(16) NOT NULL,
      market_slug VARCHAR(128) NOT NULL,
      market_condition_id VARCHAR(128) NOT NULL,
      outcome VARCHAR(16) NOT NULL,
      asset_id VARCHAR(128) NOT NULL,
      trade_count INT NOT NULL,
      trade_size DECIMAL(30,18) NOT NULL,
      trade_notional DECIMAL(30,18) NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uq_window_symbol_outcome (window_start_ts, symbol, outcome),
      KEY idx_symbol_window (symbol, window_start_ts),
      KEY idx_market_window (market_slug, window_start_ts),
      KEY idx_asset_window (asset_id, window_start_ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def upsert_rows(conn, table: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO `{table}` (
      window_start_ts,
      window_end_ts,
      symbol,
      market_slug,
      market_condition_id,
      outcome,
      asset_id,
      trade_count,
      trade_size,
      trade_notional
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      window_end_ts = VALUES(window_end_ts),
      market_slug = VALUES(market_slug),
      market_condition_id = VALUES(market_condition_id),
      asset_id = VALUES(asset_id),
      trade_count = VALUES(trade_count),
      trade_size = VALUES(trade_size),
      trade_notional = VALUES(trade_notional)
    """
    params = [
        (
            row["window_start_ts"],
            row["window_end_ts"],
            row["symbol"],
            row["market_slug"],
            row["market_condition_id"],
            row["outcome"],
            row["asset_id"],
            row["trade_count"],
            row["trade_size"],
            row["trade_notional"],
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


class TradeVolumeWatcher:
    def __init__(self, args):
        self.args = args
        self.symbols = [s.strip().lower() for s in args.symbols.split(",") if s.strip()]
        self.stop_event = threading.Event()
        self.state_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self.ws_send_lock = threading.Lock()
        self.ws_app: websocket.WebSocketApp | None = None
        self.active_bindings: dict[str, MarketBinding] = {}
        self.known_assets: dict[str, MarketBinding] = {}
        self.last_discovery_error: dict[str, str] = {}
        self.current_window_start_ts: int | None = None
        self.window_stats: dict[int, dict[tuple[str, str], dict[str, Any]]] = {}
        self.db_conn = self.create_db_conn() if args.mysql_enabled else None
        if self.db_conn is not None:
            ensure_table(self.db_conn, args.mysql_table)

    def start(self) -> None:
        discovery_thread = threading.Thread(target=self.discovery_loop, daemon=True)
        heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        discovery_thread.start()
        heartbeat_thread.start()

        if not self.wait_for_initial_assets():
            raise RuntimeError("no active asset ids discovered before stop")

        try:
            self.ws_loop()
        finally:
            self.print_current_window_summary(force=True)
            if self.db_conn is not None:
                self.db_conn.close()

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
        ws.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))
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
        if str(payload.get("event_type") or "").strip() != "last_trade_price":
            return

        asset_id = str(payload.get("asset_id") or "")
        binding = self.lookup_binding(asset_id)
        if binding is None:
            return

        trade_size = parse_decimal(payload.get("size"))
        trade_price = parse_decimal(payload.get("price"))
        self.record_trade(binding.market_start_ts, binding.symbol, binding.outcome, trade_size, trade_price)

    def record_trade(
        self,
        window_start_ts: int,
        symbol: str,
        outcome: str,
        trade_size: Decimal,
        trade_price: Decimal,
    ) -> None:
        with self.stats_lock:
            window = self.window_stats.setdefault(window_start_ts, {})
            key = (symbol, outcome)
            stats = window.setdefault(
                key,
                {
                    "count": 0,
                    "size": Decimal("0"),
                    "notional": Decimal("0"),
                },
            )
            stats["count"] += 1
            stats["size"] += trade_size
            stats["notional"] += trade_size * trade_price

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
            if removed:
                self.send_subscription_update("unsubscribe", removed)
            if added:
                self.send_subscription_update("subscribe", added)

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

        previous_summary = self.build_window_summary(previous_window_start_ts, previous_bindings, pop=True)
        self.persist_window_summary(previous_window_start_ts, previous_summary["rows"])
        print("[window]")
        print(f"  current={self.format_market_list(current_bindings)}")
        for line in previous_summary["lines"]:
            print(f"  {line}")
        self.current_window_start_ts = window_start_ts

    def build_window_summary(
        self,
        window_start_ts: int,
        bindings: dict[str, MarketBinding],
        pop: bool,
    ) -> dict[str, list]:
        with self.stats_lock:
            stats = self.window_stats.pop(window_start_ts, {}) if pop else dict(self.window_stats.get(window_start_ts, {}))

        lines = [f"window={format_window_label(window_start_ts)}"]
        rows = []
        total_count = 0
        total_size = Decimal("0")
        total_notional = Decimal("0")

        ordered_bindings = sorted(bindings.values(), key=lambda item: (item.symbol, item.outcome))
        for binding in ordered_bindings:
            outcome_stats = stats.get((binding.symbol, binding.outcome), {})
            rows.append(
                {
                    "window_start_ts": binding.market_start_ts,
                    "window_end_ts": binding.market_end_ts,
                    "symbol": binding.symbol,
                    "market_slug": binding.market_slug,
                    "market_condition_id": binding.market_condition_id,
                    "outcome": binding.outcome,
                    "asset_id": binding.asset_id,
                    "trade_count": int(outcome_stats.get("count", 0)),
                    "trade_size": str(outcome_stats.get("size", Decimal("0"))),
                    "trade_notional": str(outcome_stats.get("notional", Decimal("0"))),
                }
            )

        symbols = sorted({binding.symbol for binding in ordered_bindings})
        if not symbols:
            symbols = sorted({symbol for symbol, _outcome in stats})

        for symbol in symbols:
            symbol_count = 0
            symbol_size = Decimal("0")
            symbol_notional = Decimal("0")
            for outcome in ("Up", "Down"):
                outcome_stats = stats.get((symbol, outcome), {})
                outcome_count = int(outcome_stats.get("count", 0))
                outcome_size = outcome_stats.get("size", Decimal("0"))
                outcome_notional = outcome_stats.get("notional", Decimal("0"))
                symbol_count += outcome_count
                symbol_size += outcome_size
                symbol_notional += outcome_notional
                lines.append(
                    f"{symbol} {outcome.lower()}: trades={outcome_count} size={format_decimal(outcome_size)} notional={format_decimal(outcome_notional)}"
                )
            lines.append(
                f"{symbol} total: trades={symbol_count} size={format_decimal(symbol_size)} notional={format_decimal(symbol_notional)}"
            )
            total_count += symbol_count
            total_size += symbol_size
            total_notional += symbol_notional

        lines.insert(1, f"all total: trades={total_count} size={format_decimal(total_size)} notional={format_decimal(total_notional)}")
        return {"lines": lines, "rows": rows}

    def print_current_window_summary(self, force: bool) -> None:
        window_start_ts = self.current_window_start_ts
        if window_start_ts is None:
            return
        with self.state_lock:
            bindings = dict(self.active_bindings)
        summary = self.build_window_summary(window_start_ts, bindings, pop=False)
        if force:
            print("[window-final]")
            for line in summary["lines"]:
                print(f"  {line}")

    def format_market_list(self, bindings: dict[str, MarketBinding]) -> str:
        market_by_symbol = {}
        for binding in bindings.values():
            market_by_symbol[binding.symbol] = binding.market_slug
        if not market_by_symbol:
            return "none"
        return ",".join(f"{symbol}:{market_by_symbol[symbol]}" for symbol in sorted(market_by_symbol))

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
        try:
            app.send(json.dumps({"operation": operation, "assets_ids": asset_ids}))
        except Exception as exc:  # noqa: BLE001
            print(f"[ws] {operation} failed: {exc}")

    def create_db_conn(self):
        return pymysql.connect(
            host=self.args.mysql_host,
            port=self.args.mysql_port,
            user=self.args.mysql_user,
            password=self.args.mysql_password,
            database=self.args.mysql_database,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def persist_window_summary(self, window_start_ts: int, rows: list[dict]) -> None:
        if self.db_conn is None or not rows:
            return
        upsert_rows(self.db_conn, self.args.mysql_table, rows)
        print(f"  db_written_rows={len(rows)} table={self.args.mysql_table} window_start_ts={window_start_ts}")


def parse_args():
    load_dotenv()
    p = argparse.ArgumentParser(
        description="Watch Polymarket 5m Up/Down trade volume for the current symbols and print a summary every 5 minutes"
    )
    p.add_argument("--symbols", default=SYMBOLS_DEFAULT, help=f"Comma-separated symbols, default: {SYMBOLS_DEFAULT}")
    p.add_argument("--fetch-timeout-seconds", type=float, default=0.8, help="HTTP timeout per Gamma request")
    p.add_argument(
        "--refresh-interval-seconds",
        type=float,
        default=0.5,
        help="Refresh current 5m market bindings at this interval",
    )
    p.add_argument("--ws-url", default=WS_MARKET_URL, help=f"Polymarket market websocket URL, default: {WS_MARKET_URL}")
    p.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--mysql-database", default=os.getenv("MYSQL_DATABASE", ""))
    p.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", ""))
    p.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", ""))
    p.add_argument("--mysql-table", default=os.getenv("POLYMARKET_TRADE_VOLUME_TABLE", DEFAULT_TABLE))
    args = p.parse_args()
    args.mysql_enabled = bool(args.mysql_database and args.mysql_user and args.mysql_password)
    return args


def main():
    args = parse_args()
    watcher = TradeVolumeWatcher(args)

    def handle_stop(_signum, _frame):
        print("[main] stop signal received")
        watcher.stop()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    print(f"symbols: {args.symbols}")
    print(f"ws_url: {args.ws_url}")
    print(f"mysql_enabled: {args.mysql_enabled}")
    if args.mysql_enabled:
        print(f"mysql_table: {args.mysql_table}")
    watcher.start()


if __name__ == "__main__":
    main()
