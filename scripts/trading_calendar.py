#!/usr/bin/env python3
"""Trading-date helpers shared by the updater and validator."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo


BANGKOK = ZoneInfo("Asia/Bangkok")
DEFAULT_EXTRA_HOLIDAYS = (
    Path(__file__).resolve().parents[1] / "config" / "tfex_holidays.txt"
)


def load_extra_holidays(path: Path = DEFAULT_EXTRA_HOLIDAYS) -> set[date]:
    """Load exchange-only closure dates from a simple ISO-date text file."""
    if not path.exists():
        return set()

    result: set[date] = set()
    for line_number, raw_line in enumerate(path.read_text().splitlines(), start=1):
        value = raw_line.split("#", 1)[0].strip()
        if not value:
            continue
        try:
            result.add(date.fromisoformat(value))
        except ValueError as exc:
            raise ValueError(
                f"Invalid TFEX holiday on line {line_number}: {value!r}"
            ) from exc
    return result


def expected_trading_date(
    as_of: date | datetime | None = None,
    closed_dates: Iterable[date] | None = None,
) -> date:
    """Return the latest completed Thai trading date before ``as_of``."""
    if as_of is None:
        local_date = datetime.now(BANGKOK).date()
    elif isinstance(as_of, datetime):
        local_date = (
            as_of.astimezone(BANGKOK).date()
            if as_of.tzinfo
            else as_of.date()
        )
    else:
        local_date = as_of

    if closed_dates is None:
        import holidays

        years = {local_date.year, (local_date - timedelta(days=10)).year}
        closed = set(holidays.country_holidays("TH", years=years).keys())
        closed.update(load_extra_holidays())
    else:
        closed = set(closed_dates)

    candidate = local_date - timedelta(days=1)
    while candidate.weekday() >= 5 or candidate in closed:
        candidate -= timedelta(days=1)
    return candidate
