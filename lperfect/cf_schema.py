# -*- coding: utf-8 -*-
"""CF schema constants and helpers for LPERFECT inputs."""

from __future__ import annotations

# Import typing primitives.
from typing import Iterable

# Import numpy for time decoding.
import numpy as np

CF_CONVENTIONS = "CF-1.10"

RAIN_TIME_UNITS = "hours since 1900-01-01 00:00:0.0"
RAIN_TIME_VAR = "time"
RAIN_LAT_VAR = "latitude"
RAIN_LON_VAR = "longitude"
RAIN_RATE_VAR = "rain_rate"
RAIN_RATE_UNITS = "mm h-1"
RAIN_CRS_VAR = "crs"
RAIN_GRID_MAPPING_ATTR = "grid_mapping"


def normalize_cf_time_units(units: str | None) -> str:
    """Normalize CF time units for comparison."""
    if units is None:
        return ""
    return units.strip()


def hours_since_1900_to_datetime64(values: Iterable[float] | np.ndarray) -> np.ndarray:
    """Convert numeric hours since 1900-01-01 to numpy.datetime64 array."""
    hours = np.asarray(values, dtype=np.float64)
    base = np.datetime64("1900-01-01T00:00:00")
    seconds = hours * 3600.0
    return base + (seconds * np.timedelta64(1, "s"))
