# -*- coding: utf-8 -*-
"""Rain input and blending (rank0 read, broadcast)."""  # execute statement

# Import dataclass for structured source definition.
from dataclasses import dataclass  # import dataclasses import dataclass

# Import typing primitives.
from typing import Any, Dict, List, Optional, Tuple  # import typing import Any, Dict, List, Optional, Tuple

# Import numpy.
import numpy as np  # import numpy as np

# Import xarray.
import xarray as xr  # import xarray as xr


@dataclass  # apply decorator
class RainSource:  # define class RainSource
    """Rainfall source configuration."""  # execute statement
    name: str  # execute statement
    kind: str  # execute statement
    weight: float  # execute statement
    mode: str  # execute statement
    path: Optional[str] = None  # execute statement
    var: Optional[str] = None  # execute statement
    time_var: str = "time"  # execute statement
    select: str = "nearest"  # execute statement
    value: Optional[float] = None  # execute statement


# Simple cache to avoid reopening NetCDF files each step.
_NC_CACHE: Dict[str, xr.Dataset] = {}  # execute statement


def xr_open_cached(path: str) -> xr.Dataset:  # define function xr_open_cached
    """Open a NetCDF dataset with caching."""  # execute statement
    if path not in _NC_CACHE:  # check condition path not in _NC_CACHE:
        _NC_CACHE[path] = xr.open_dataset(path)  # execute statement
    return _NC_CACHE[path]  # return _NC_CACHE[path]


def xr_close_cache() -> None:  # define function xr_close_cache
    """Close all cached datasets."""  # execute statement
    for ds in _NC_CACHE.values():  # loop over ds in _NC_CACHE.values():
        try:  # start exception handling
            ds.close()  # execute statement
        except Exception:  # handle exception Exception:
            pass  # no-op placeholder
    _NC_CACHE.clear()  # execute statement


def build_rain_sources(cfg: Dict[str, Any]) -> List[RainSource]:  # define function build_rain_sources
    """Parse cfg['rain']['sources'] into a list of RainSource."""  # execute statement
    sources_cfg = cfg.get("rain", {}).get("sources", {})  # set sources_cfg
    out: List[RainSource] = []  # execute statement
    for name, sc in sources_cfg.items():  # loop over name, sc in sources_cfg.items():
        out.append(RainSource(  # execute statement
            name=name,  # set name
            kind=str(sc.get("kind", "netcdf")),  # set kind
            weight=float(sc.get("weight", 0.0)),  # set weight
            mode=str(sc.get("mode", "intensity_mmph")),  # set mode
            path=sc.get("path", None),  # set path
            var=sc.get("var", None),  # set var
            time_var=str(sc.get("time_var", "time")),  # set time_var
            select=str(sc.get("select", "nearest")),  # set select
            value=sc.get("value", None),  # set value
        ))  # execute statement
    return out  # return out


def rain_to_step_mm(field: np.ndarray, mode: str, dt_s: float) -> np.ndarray:  # define function rain_to_step_mm
    """Convert rainfall field to mm per model step."""  # execute statement
    f = np.asarray(field, dtype=np.float64)  # set f
    f = np.where(np.isfinite(f), f, 0.0)  # set f
    f = np.maximum(f, 0.0)  # set f
    if mode == "intensity_mmph":  # check condition mode == "intensity_mmph":
        return f * (dt_s / 3600.0)  # return f * (dt_s / 3600.0)
    if mode == "depth_mm_per_step":  # check condition mode == "depth_mm_per_step":
        return f  # return f
    raise ValueError(f"Unknown rain mode '{mode}'")  # raise ValueError(f"Unknown rain mode '{mode}'")


def pick_time_index(time_vals: np.ndarray, target: np.datetime64) -> int:  # define function pick_time_index
    """Pick nearest time index for a datetime64 time axis."""  # execute statement
    tv = np.asarray(time_vals)  # set tv
    if tv.dtype.kind != "M":  # check condition tv.dtype.kind != "M":
        raise ValueError("Time axis is not datetime64; use select='step' or enable CF time decoding.")  # raise ValueError("Time axis is not datetime64; use select='step' or enable CF time decoding.")
    return int(np.argmin(np.abs(tv - target)))  # return int(np.argmin(np.abs(tv - target)))


def blended_rain_step_mm_rank0(  # define function blended_rain_step_mm_rank0
    sources: List[RainSource],  # execute statement
    shape: Tuple[int, int],  # execute statement
    dt_s: float,  # execute statement
    step_idx: int,  # execute statement
    sim_time: Optional[np.datetime64],  # execute statement
) -> np.ndarray:  # execute statement
    """Compute blended rainfall (mm/step) on rank0."""  # execute statement
    H, W = shape  # set H, W
    total = np.zeros((H, W), dtype=np.float64)  # set total

    for src in sources:  # loop over src in sources:
        if src.weight == 0.0:  # check condition src.weight == 0.0:
            continue  # continue loop

        if src.kind == "scalar":  # check condition src.kind == "scalar":
            if src.value is None:  # check condition src.value is None:
                raise ValueError(f"Rain source '{src.name}' scalar requires 'value'")  # raise ValueError(f"Rain source '{src.name}' scalar requires 'value'")
            field = np.full((H, W), float(src.value), dtype=np.float64)  # set field

        elif src.kind == "netcdf":  # check alternate condition src.kind == "netcdf":
            if not src.path or not src.var:  # check condition not src.path or not src.var:
                raise ValueError(f"Rain source '{src.name}' netcdf requires 'path' and 'var'")  # raise ValueError(f"Rain source '{src.name}' netcdf requires 'path' and 'var'")
            ds = xr_open_cached(src.path)  # set ds
            if src.var not in ds:  # check condition src.var not in ds:
                raise ValueError(f"Rain variable '{src.var}' not found in {src.path}")  # raise ValueError(f"Rain variable '{src.var}' not found in {src.path}")
            da = ds[src.var]  # set da

            if da.ndim == 2:  # check condition da.ndim == 2:
                field = np.asarray(da.values)  # set field
            elif da.ndim == 3:  # check alternate condition da.ndim == 3:
                tdim = src.time_var if src.time_var in da.dims else da.dims[0]  # set tdim
                if src.select == "step" or sim_time is None:  # check condition src.select == "step" or sim_time is None:
                    it = min(step_idx, da.sizes[tdim] - 1)  # set it
                else:  # fallback branch
                    it = pick_time_index(np.asarray(ds[tdim].values), sim_time)  # set it
                field = np.asarray(da.isel({tdim: it}).values)  # set field
            else:  # fallback branch
                raise ValueError("Rain var must be 2D or 3D (time,y,x)")  # raise ValueError("Rain var must be 2D or 3D (time,y,x)")

            if field.shape != (H, W):  # check condition field.shape != (H, W):
                raise ValueError(f"Rain shape {field.shape} != domain shape {(H, W)}")  # raise ValueError(f"Rain shape {field.shape} != domain shape {(H, W)}")

        else:  # fallback branch
            raise ValueError(f"Unknown rain kind '{src.kind}' for '{src.name}'")  # raise ValueError(f"Unknown rain kind '{src.kind}' for '{src.name}'")

        total += src.weight * rain_to_step_mm(field, src.mode, dt_s)  # execute statement

    return total  # return total
