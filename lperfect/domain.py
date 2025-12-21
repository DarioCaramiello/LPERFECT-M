# -*- coding: utf-8 -*-
"""Domain reading and broadcasting (NetCDF-only)."""

# Import typing primitives.
from typing import Any, Dict, Optional

# Import dataclass for structured domain object.
from dataclasses import dataclass

# Import numpy for arrays.
import numpy as np

# Import xarray for NetCDF reading.
import xarray as xr


@dataclass
class Domain:
    """Domain data required by the model."""
    dem: np.ndarray
    d8: np.ndarray
    cn: np.ndarray
    channel_mask: Optional[np.ndarray]
    active_mask: np.ndarray
    x_name: str
    y_name: str
    x_vals: np.ndarray
    y_vals: np.ndarray
    grid_mapping_name: Optional[str]
    grid_mapping_attrs: Dict[str, Any]
    cell_area_m2: float | np.ndarray


def cell_area_m2_from_domain(ds: xr.Dataset, x_name: str, y_name: str) -> float | np.ndarray:
    """Estimate cell area from coordinate spacing and units.

    If x/y units look like meters -> constant area dx*dy.
    If x/y are degrees -> compute per-row geodesic area (WGS84) when possible.
    """
    # Extract coordinate vectors.
    x = np.asarray(ds[x_name].values)
    y = np.asarray(ds[y_name].values)
    # Estimate spacings robustly via median.
    dx = float(np.median(np.abs(np.diff(x))))
    dy = float(np.median(np.abs(np.diff(y))))
    # Read units (if any).
    xu = str(ds[x_name].attrs.get("units", "")).lower()
    yu = str(ds[y_name].attrs.get("units", "")).lower()

    # Projected meters case -> constant area.
    if ("m" in xu) and ("m" in yu):
        return float(dx * dy)

    # Geographic degrees case -> compute per-row areas.
    H = int(ds.dims[y_name])
    W = int(ds.dims[x_name])

    # Try pyproj Geod for accurate ellipsoidal area.
    try:
        from pyproj import Geod
        geod = Geod(ellps="WGS84")
        # Build one cell polygon width using dx.
        lon_left = float(x.min())
        lon_right = float(lon_left + (dx if x[1] > x[0] else -dx))
        # Allocate per-row areas.
        areas_row = np.zeros(H, dtype=np.float64)
        # Loop rows (y dimension).
        for i in range(H):
            lat_top = float(y[i])
            lat_bot = float(lat_top - dy if y[1] < y[0] else lat_top + dy)
            lons = [lon_left, lon_right, lon_right, lon_left]
            lats = [lat_top, lat_top, lat_bot, lat_bot]
            poly_area, _ = geod.polygon_area_perimeter(lons, lats)
            areas_row[i] = abs(poly_area)
        # Expand to full grid.
        return np.repeat(areas_row[:, None], W, axis=1)
    except Exception:
        # Fallback: simple spherical approximation.
        R = 6371000.0
        dlon = np.deg2rad(abs(dx))
        areas_row = np.zeros(H, dtype=np.float64)
        for i in range(H):
            lat_top = np.deg2rad(float(y[i]))
            lat_bot = np.deg2rad(float(y[i] - dy if y[1] < y[0] else y[i] + dy))
            areas_row[i] = abs((R * R) * dlon * (np.sin(lat_bot) - np.sin(lat_top)))
        return np.repeat(areas_row[:, None], W, axis=1)


def read_domain_netcdf_rank0(cfg: Dict[str, Any]) -> Domain:
    """Read domain NetCDF on rank 0."""
    # Extract domain configuration.
    dom_cfg = cfg["domain"]
    path = dom_cfg["domain_nc"]
    varmap = dom_cfg.get("varmap", {})

    # Open dataset.
    ds = xr.open_dataset(path)

    # Resolve variable names.
    dem_name = varmap.get("dem", "dem")
    d8_name = varmap.get("d8", "d8")
    cn_name = varmap.get("cn", "cn")
    ch_name = varmap.get("channel_mask", "channel_mask")

    # Read arrays.
    dem = np.asarray(ds[dem_name].values).astype(np.float64)
    d8 = np.asarray(ds[d8_name].values).astype(np.int32)
    cn = np.asarray(ds[cn_name].values).astype(np.float64)

    # Optional channel mask.
    channel_mask = None
    if ch_name in ds:
        channel_mask = (np.asarray(ds[ch_name].values) > 0)

    # Active mask based on valid DEM values.
    active_mask = np.isfinite(dem)

    # Resolve coordinate names.
    x_name = varmap.get("x", "x")
    y_name = varmap.get("y", "y")

    # Read coordinates from coords or variables.
    x_vals = np.asarray(ds.coords[x_name].values if x_name in ds.coords else ds[x_name].values)
    y_vals = np.asarray(ds.coords[y_name].values if y_name in ds.coords else ds[y_name].values)

    # Estimate cell area.
    cell_area_m2 = cell_area_m2_from_domain(ds, x_name=x_name, y_name=y_name)

    # Preserve CF grid mapping if present.
    gm_name = ds[dem_name].attrs.get("grid_mapping", None)
    gm_attrs: Dict[str, Any] = {}
    if gm_name and gm_name in ds:
        gm_attrs = dict(ds[gm_name].attrs)

    # Close dataset.
    ds.close()

    # Clean CN outside active cells.
    cn = np.where(active_mask & np.isfinite(cn), cn, 0.0)

    # Apply active mask to channel mask if present.
    if channel_mask is not None:
        channel_mask = channel_mask & active_mask

    # Return domain object.
    return Domain(
        dem=dem,
        d8=d8,
        cn=cn,
        channel_mask=channel_mask,
        active_mask=active_mask,
        x_name=x_name,
        y_name=y_name,
        x_vals=x_vals,
        y_vals=y_vals,
        grid_mapping_name=gm_name,
        grid_mapping_attrs=gm_attrs,
        cell_area_m2=cell_area_m2,
    )


def bcast_domain(comm, dom0: Optional[Domain]) -> Domain:
    """Broadcast Domain from rank0 to all ranks."""
    # Rank id.
    rank = comm.Get_rank()

    # Prepare metadata dict on root.
    if rank == 0:
        meta = {
            "shape": dom0.dem.shape,
            "x_name": dom0.x_name,
            "y_name": dom0.y_name,
            "x_vals": dom0.x_vals,
            "y_vals": dom0.y_vals,
            "grid_mapping_name": dom0.grid_mapping_name,
            "grid_mapping_attrs": dom0.grid_mapping_attrs,
            "has_channel_mask": dom0.channel_mask is not None,
            "cell_area_is_scalar": bool(np.isscalar(dom0.cell_area_m2)),
            "cell_area_scalar": float(dom0.cell_area_m2) if np.isscalar(dom0.cell_area_m2) else None,
        }
    else:
        meta = None

    # Broadcast metadata (pickle-based).
    meta = comm.bcast(meta, root=0)

    # Extract shape.
    H, W = meta["shape"]

    # Allocate or reuse arrays.
    if rank != 0:
        dem = np.empty((H, W), dtype=np.float64)
        d8 = np.empty((H, W), dtype=np.int32)
        cn = np.empty((H, W), dtype=np.float64)
        active = np.empty((H, W), dtype=np.bool_)
    else:
        dem = dom0.dem.astype(np.float64)
        d8 = dom0.d8.astype(np.int32)
        cn = dom0.cn.astype(np.float64)
        active = dom0.active_mask.astype(np.bool_)

    # Broadcast arrays.
    comm.Bcast(dem, root=0)
    comm.Bcast(d8, root=0)
    comm.Bcast(cn, root=0)
    comm.Bcast(active, root=0)

    # Optional channel mask.
    channel_mask = None
    if meta["has_channel_mask"]:
        if rank != 0:
            cm = np.empty((H, W), dtype=np.bool_)
        else:
            cm = dom0.channel_mask.astype(np.bool_)
        comm.Bcast(cm, root=0)
        channel_mask = cm

    # Cell area.
    if meta["cell_area_is_scalar"]:
        cell_area_m2: float | np.ndarray = float(meta["cell_area_scalar"])
    else:
        if rank != 0:
            ca = np.empty((H, W), dtype=np.float64)
        else:
            ca = dom0.cell_area_m2.astype(np.float64)
        comm.Bcast(ca, root=0)
        cell_area_m2 = ca

    # Return domain.
    return Domain(
        dem=dem,
        d8=d8,
        cn=cn,
        channel_mask=channel_mask,
        active_mask=active,
        x_name=meta["x_name"],
        y_name=meta["y_name"],
        x_vals=np.asarray(meta["x_vals"]),
        y_vals=np.asarray(meta["y_vals"]),
        grid_mapping_name=meta["grid_mapping_name"],
        grid_mapping_attrs=dict(meta["grid_mapping_attrs"]),
        cell_area_m2=cell_area_m2,
    )
