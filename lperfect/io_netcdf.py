# -*- coding: utf-8 -*-
"""NetCDF I/O for outputs and restart state (rank0)."""

# Import JSON for embedding config as provenance attribute.
import json

# Import typing primitives.
from typing import Any, Dict

# Import numpy.
import numpy as np

# Import xarray.
import xarray as xr

# Import local time helper.
from .time_utils import utc_now_iso

# Import local Domain and Particles.
from .domain import Domain
from .particles import Particles


def write_results_netcdf_rank0(out_path: str, cfg: Dict[str, Any], dom: Domain,
                              flood_depth_m: np.ndarray, risk_index: np.ndarray) -> None:
    """Write final results in CF-friendly NetCDF."""
    out_cfg = cfg.get("output", {})

    ds = xr.Dataset()

    ds = ds.assign_coords({
        dom.x_name: xr.DataArray(dom.x_vals, dims=(dom.x_name,)),
        dom.y_name: xr.DataArray(dom.y_vals, dims=(dom.y_name,)),
    })

    ds["flood_depth"] = xr.DataArray(
        flood_depth_m.astype(np.float32),
        dims=(dom.y_name, dom.x_name),
        attrs={
            "standard_name": "water_depth",
            "long_name": "flooded_water_depth",
            "units": "m",
        },
    )

    ds["risk_index"] = xr.DataArray(
        risk_index.astype(np.float32),
        dims=(dom.y_name, dom.x_name),
        attrs={
            "long_name": "hydrogeological_risk_index",
            "units": "1",
        },
    )

    if dom.grid_mapping_name and dom.grid_mapping_attrs:
        gm = dom.grid_mapping_name
        ds[gm] = xr.DataArray(0, attrs=dom.grid_mapping_attrs)
        ds["flood_depth"].attrs["grid_mapping"] = gm
        ds["risk_index"].attrs["grid_mapping"] = gm

    ds.attrs["title"] = out_cfg.get("title", "LPERFECT results")
    ds.attrs["institution"] = out_cfg.get("institution", "")
    ds.attrs["source"] = "LPERFECT"
    ds.attrs["history"] = f"{utc_now_iso()}: results written by LPERFECT"
    ds.attrs["Conventions"] = out_cfg.get("Conventions", "CF-1.10")
    ds.attrs["lperfect_config_json"] = json.dumps(cfg, separators=(",", ":"), sort_keys=True)

    ds.to_netcdf(out_path)


def save_restart_netcdf_rank0(out_path: str, cfg: Dict[str, Any], dom: Domain,
                             elapsed_s: float, cum_rain_vol_m3: float, cum_runoff_vol_m3: float, cum_outflow_vol_m3: float,
                             P_cum_mm_full: np.ndarray, Q_cum_mm_full: np.ndarray, particles_all: Particles) -> None:
    """Save restart state to NetCDF."""
    ds = xr.Dataset()

    ds = ds.assign_coords({
        dom.x_name: xr.DataArray(dom.x_vals, dims=(dom.x_name,)),
        dom.y_name: xr.DataArray(dom.y_vals, dims=(dom.y_name,)),
        "particle": xr.DataArray(np.arange(particles_all.r.size, dtype=np.int64), dims=("particle",)),
    })

    ds["P_cum_mm"] = xr.DataArray(P_cum_mm_full.astype(np.float64), dims=(dom.y_name, dom.x_name), attrs={"units": "mm"})
    ds["Q_cum_mm"] = xr.DataArray(Q_cum_mm_full.astype(np.float64), dims=(dom.y_name, dom.x_name), attrs={"units": "mm"})

    ds["particle_r"] = xr.DataArray(particles_all.r.astype(np.int32), dims=("particle",), attrs={"units": "1"})
    ds["particle_c"] = xr.DataArray(particles_all.c.astype(np.int32), dims=("particle",), attrs={"units": "1"})
    ds["particle_vol"] = xr.DataArray(particles_all.vol.astype(np.float64), dims=("particle",), attrs={"units": "m3"})
    ds["particle_tau"] = xr.DataArray(particles_all.tau.astype(np.float64), dims=("particle",), attrs={"units": "s"})

    ds["elapsed_s"] = xr.DataArray(np.array(elapsed_s, dtype=np.float64), attrs={"units": "s"})
    ds["cum_rain_vol_m3"] = xr.DataArray(np.array(cum_rain_vol_m3, dtype=np.float64), attrs={"units": "m3"})
    ds["cum_runoff_vol_m3"] = xr.DataArray(np.array(cum_runoff_vol_m3, dtype=np.float64), attrs={"units": "m3"})
    ds["cum_outflow_vol_m3"] = xr.DataArray(np.array(cum_outflow_vol_m3, dtype=np.float64), attrs={"units": "m3"})

    if dom.grid_mapping_name and dom.grid_mapping_attrs:
        gm = dom.grid_mapping_name
        ds[gm] = xr.DataArray(0, attrs=dom.grid_mapping_attrs)
        ds["P_cum_mm"].attrs["grid_mapping"] = gm
        ds["Q_cum_mm"].attrs["grid_mapping"] = gm

    ds.attrs["title"] = "LPERFECT restart"
    ds.attrs["source"] = "LPERFECT"
    ds.attrs["history"] = f"{utc_now_iso()}: restart written by LPERFECT"
    ds.attrs["Conventions"] = cfg.get("output", {}).get("Conventions", "CF-1.10")
    ds.attrs["lperfect_config_json"] = json.dumps(cfg, separators=(",", ":"), sort_keys=True)

    ds.to_netcdf(out_path)


def load_restart_netcdf_rank0(path: str) -> Dict[str, Any]:
    """Load restart NetCDF and return state dict."""
    ds = xr.open_dataset(path)

    out = {
        "P_cum_mm": np.asarray(ds["P_cum_mm"].values).astype(np.float64),
        "Q_cum_mm": np.asarray(ds["Q_cum_mm"].values).astype(np.float64),
        "r": np.asarray(ds["particle_r"].values).astype(np.int32),
        "c": np.asarray(ds["particle_c"].values).astype(np.int32),
        "vol": np.asarray(ds["particle_vol"].values).astype(np.float64),
        "tau": np.asarray(ds["particle_tau"].values).astype(np.float64),
        "elapsed_s": float(ds["elapsed_s"].values),
        "cum_rain_vol_m3": float(ds["cum_rain_vol_m3"].values),
        "cum_runoff_vol_m3": float(ds["cum_runoff_vol_m3"].values),
        "cum_outflow_vol_m3": float(ds["cum_outflow_vol_m3"].values),
    }

    ds.close()
    return out
