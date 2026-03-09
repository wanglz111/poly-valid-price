#!/usr/bin/env python3
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

SAFE_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
TRUTHY_VALUES = {"1", "true", "yes", "on"}
FALSY_VALUES = {"0", "false", "no", "off"}


def parse_bool_flag(value) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in TRUTHY_VALUES:
        return True
    if normalized in FALSY_VALUES:
        return False
    raise ValueError(f"Invalid boolean flag: {value}")


def ensure_safe_table_name(table_name: str) -> str:
    if not SAFE_TABLE_NAME_RE.fullmatch(table_name):
        raise ValueError(f"Unsafe table name: {table_name}")
    return table_name


def parse_timezone_offset(value: str) -> timezone:
    raw = value.strip().upper()
    if raw in {"Z", "UTC", "+00:00", "+0000", "+00"}:
        return timezone.utc

    sign = 1
    if raw.startswith("+"):
        raw = raw[1:]
    elif raw.startswith("-"):
        sign = -1
        raw = raw[1:]

    if ":" in raw:
        hours_text, minutes_text = raw.split(":", 1)
    elif len(raw) in {1, 2}:
        hours_text, minutes_text = raw, "00"
    elif len(raw) == 4:
        hours_text, minutes_text = raw[:2], raw[2:]
    else:
        raise ValueError(f"Invalid timezone offset: {value}")

    hours = int(hours_text)
    minutes = int(minutes_text)
    if hours > 23 or minutes > 59:
        raise ValueError(f"Invalid timezone offset: {value}")

    total_minutes = sign * (hours * 60 + minutes)
    return timezone(timedelta(minutes=total_minutes))


@dataclass(frozen=True)
class TableNameResolver:
    daily_enabled: bool
    table_timezone: timezone

    def resolve(self, base_table: str, ts_unix: int) -> str:
        safe_base = ensure_safe_table_name(base_table)
        if not self.daily_enabled:
            return safe_base
        target_dt = datetime.fromtimestamp(ts_unix, tz=self.table_timezone)
        return f"{safe_base}_{target_dt:%Y%m%d}"
