# -*- coding: utf-8 -*-
"""Time helpers for LPERFECT."""

# Import datetime helpers.
from datetime import datetime, timezone

# Import numpy for datetime64 parsing.
import numpy as np


def utc_now_iso() -> str:
    """Return current UTC time as an ISO-8601 string with 'Z'."""
    # Get current time in UTC.
    now = datetime.now(timezone.utc)
    # Convert to ISO string and force 'Z' suffix.
    return now.isoformat().replace("+00:00", "Z")


def parse_iso8601_to_datetime64(s: str | None) -> np.datetime64 | None:
    """Parse an ISO-8601 timestamp into numpy.datetime64.

    Notes
    -----
    - Accepts 'Z' suffix.
    - Returns None if s is None/empty.
    """
    # Handle null / empty.
    if not s:
        return None
    # Strip whitespace.
    ss = s.strip()
    # Replace Z with explicit UTC offset for numpy.
    if ss.endswith("Z"):
        ss = ss[:-1] + "+00:00"
    # Convert to numpy datetime64 (timezone-aware strings are accepted by numpy).
    return np.datetime64(ss)
