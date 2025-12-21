# -*- coding: utf-8 -*-
"""Rain input and blending (rank0 read, broadcast)."""

# Import dataclass for structured source definition.
from dataclasses import dataclass

# Import typing primitives.
from typing import Any, Dict, List, Optional, Tuple

# Import numpy.
import numpy as np

# Import xarray.
import xarray as xr


@dataclass
class RainSource:
    """Rainfall source configuration."""
    name: str
    kind: str
    weight: float
    mode: str
    path: Optional[str] = None
    var: Optional[str] = None
    time_var: str = "time"
    select: str = "nearest"
    value: Optional[float] = None


# Simple cache to avoid reopening NetCDF files each step.
_NC_CACHE: Dict[str, xr.Dataset] = {}


def xr_open_cached(path: str) -> xr.Dataset:
    """Open a NetCDF dataset with caching."""
    if path not in _NC_CACHE:
        _NC_CACHE[path] = xr.open_dataset(path)
    return _NC_CACHE[path]


def xr_close_cache() -> None:
    """Close all cached datasets."""
    for ds in _NC_CACHE.values():
        try:
            ds.close()
        except Exception:
            pass
    _NC_CACHE.clear()


def build_rain_sources(cfg: Dict[str, Any]) -> List[RainSource]:
    """Parse cfg['rain']['sources'] into a list of RainSource."""
    sources_cfg = cfg.get("rain", {}).get("sources", {})
    out: List[RainSource] = []
    for name, sc in sources_cfg.items():
        out.append(RainSource(
            name=name,
            kind=str(sc.get("kind", "netcdf")),
            weight=float(sc.get("weight", 0.0)),
            mode=str(sc.get("mode", "intensity_mmph")),
            path=sc.get("path", None),
            var=sc.get("var", None),
            time_var=str(sc.get("time_var", "time")),
            select=str(sc.get("select", "nearest")),
            value=sc.get("value", None),
        ))
    return out


def rain_to_step_mm(field: np.ndarray, mode: str, dt_s: float) -> np.ndarray:
    """Convert rainfall field to mm per model step."""
    f = np.asarray(field, dtype=np.float64)
    f = np.where(np.isfinite(f), f, 0.0)
    f = np.maximum(f, 0.0)
    if mode == "intensity_mmph":
        return f * (dt_s / 3600.0)
    if mode == "depth_mm_per_step":
        return f
    raise ValueError(f"Unknown rain mode '{mode}'")


def pick_time_index(time_vals: np.ndarray, target: np.datetime64) -> int:
    """Pick nearest time index for a datetime64 time axis."""
    tv = np.asarray(time_vals)
    if tv.dtype.kind != "M":
        raise ValueError("Time axis is not datetime64; use select='step' or enable CF time decoding.")
    return int(np.argmin(np.abs(tv - target)))


def blended_rain_step_mm_rank0(
    sources: List[RainSource],
    shape: Tuple[int, int],
    dt_s: float,
    step_idx: int,
    sim_time: Optional[np.datetime64],
) -> np.ndarray:
    """Compute blended rainfall (mm/step) on rank0."""
    H, W = shape
    total = np.zeros((H, W), dtype=np.float64)

    for src in sources:
        if src.weight == 0.0:
            continue

        if src.kind == "scalar":
            if src.value is None:
                raise ValueError(f"Rain source '{src.name}' scalar requires 'value'")
            field = np.full((H, W), float(src.value), dtype=np.float64)

        elif src.kind == "netcdf":
            if not src.path or not src.var:
                raise ValueError(f"Rain source '{src.name}' netcdf requires 'path' and 'var'")
            ds = xr_open_cached(src.path)
            if src.var not in ds:
                raise ValueError(f"Rain variable '{src.var}' not found in {src.path}")
            da = ds[src.var]

            if da.ndim == 2:
                field = np.asarray(da.values)
            elif da.ndim == 3:
                tdim = src.time_var if src.time_var in da.dims else da.dims[0]
                if src.select == "step" or sim_time is None:
                    it = min(step_idx, da.sizes[tdim] - 1)
                else:
                    it = pick_time_index(np.asarray(ds[tdim].values), sim_time)
                field = np.asarray(da.isel({tdim: it}).values)
            else:
                raise ValueError("Rain var must be 2D or 3D (time,y,x)")

            if field.shape != (H, W):
                raise ValueError(f"Rain shape {field.shape} != domain shape {(H, W)}")

        else:
            raise ValueError(f"Unknown rain kind '{src.kind}' for '{src.name}'")

        total += src.weight * rain_to_step_mm(field, src.mode, dt_s)

    return total
