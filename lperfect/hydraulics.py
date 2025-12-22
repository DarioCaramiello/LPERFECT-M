# -*- coding: utf-8 -*-
"""Lagrangian routing helpers: particle spawning and advection."""  # execute statement

# NOTE: Rain NetCDF inputs follow cdl/rain_time_dependent.cdl (CF-1.10).

# Import typing primitives.
from typing import Optional, Tuple  # import typing import Optional, Tuple

# Import numpy.
import numpy as np  # import numpy as np

# Import local particle structures.
from .particles import Particles, empty_particles  # import .particles import Particles, empty_particles


def cell_area_at(area: float | np.ndarray, rr: np.ndarray, cc: np.ndarray) -> np.ndarray:  # define function cell_area_at
    """Return cell area(s) for indices rr,cc."""  # execute statement
    if np.isscalar(area):  # check condition np.isscalar(area):
        return np.full(rr.shape, float(area), dtype=np.float64)  # return np.full(rr.shape, float(area), dtype=np.float64)
    return area[rr, cc].astype(np.float64)  # return area[rr, cc].astype(np.float64)


def spawn_particles_from_runoff_slab(  # define function spawn_particles_from_runoff_slab
    runoff_depth_m_slab: np.ndarray,  # execute statement
    r0: int,  # execute statement
    cell_area_m2: float | np.ndarray,  # execute statement
    particle_vol_m3: float,  # execute statement
    active_mask_global: np.ndarray,  # execute statement
) -> Tuple[Particles, float]:  # execute statement
    """Convert incremental runoff depth (m) into particles for a local slab."""  # execute statement
    depth = np.maximum(runoff_depth_m_slab, 0.0)  # set depth
    rr_local, cc = np.nonzero(depth > 0.0)  # set rr_local, cc
    if rr_local.size == 0:  # check condition rr_local.size == 0:
        return empty_particles(), 0.0  # return empty_particles(), 0.0

    rr_global = rr_local + r0  # set rr_global

    ok = active_mask_global[rr_global, cc]  # set ok
    rr_local = rr_local[ok]  # set rr_local
    rr_global = rr_global[ok]  # set rr_global
    cc = cc[ok]  # set cc
    if rr_global.size == 0:  # check condition rr_global.size == 0:
        return empty_particles(), 0.0  # return empty_particles(), 0.0

    area = cell_area_at(cell_area_m2, rr_global, cc)  # set area
    vols = depth[rr_local, cc] * area  # set vols
    total_vol = float(vols.sum())  # set total_vol

    n = np.maximum(1, np.round(vols / particle_vol_m3).astype(np.int32))  # set n

    r = np.repeat(rr_global.astype(np.int32), n)  # set r
    c = np.repeat(cc.astype(np.int32), n)  # set c
    vol = np.repeat((vols / n).astype(np.float64), n)  # set vol
    tau = np.zeros_like(vol, dtype=np.float64)  # set tau

    return Particles(r=r, c=c, vol=vol, tau=tau), total_vol  # return Particles(r=r, c=c, vol=vol, tau=tau), total_vol


def advect_particles_one_step(  # define function advect_particles_one_step
    particles: Particles,  # execute statement
    valid: np.ndarray,  # execute statement
    ds_r: np.ndarray,  # execute statement
    ds_c: np.ndarray,  # execute statement
    dt_s: float,  # execute statement
    travel_time_s: float,  # execute statement
    travel_time_channel_s: float,  # execute statement
    channel_mask: Optional[np.ndarray],  # execute statement
    outflow_sink: bool,  # execute statement
) -> Tuple[Particles, float, int]:  # execute statement
    """Advance particles one step, with travel-time gating."""  # execute statement
    if particles.r.size == 0:  # check condition particles.r.size == 0:
        return particles, 0.0, 0  # return particles, 0.0, 0

    particles.tau = particles.tau - dt_s  # execute statement

    can_move = (particles.tau <= 0.0)  # set can_move
    if not np.any(can_move):  # check condition not np.any(can_move):
        return particles, 0.0, 0  # return particles, 0.0, 0

    idx = np.nonzero(can_move)[0]  # set idx
    r0 = particles.r[idx]  # set r0
    c0 = particles.c[idx]  # set c0

    v = valid[r0, c0]  # set v
    nhops = int(np.count_nonzero(v))  # set nhops

    if np.any(v):  # check condition np.any(v):
        rds = ds_r[r0[v], c0[v]]  # set rds
        cds = ds_c[r0[v], c0[v]]  # set cds
        moved = idx[v]  # set moved

        particles.r[moved] = rds  # execute statement
        particles.c[moved] = cds  # execute statement

        if channel_mask is not None:  # check condition channel_mask is not None:
            is_ch = channel_mask[rds, cds]  # set is_ch
            particles.tau[moved] = particles.tau[moved] + np.where(is_ch, travel_time_channel_s, travel_time_s)  # execute statement
        else:  # fallback branch
            particles.tau[moved] = particles.tau[moved] + travel_time_s  # execute statement

    outflow_vol = 0.0  # set outflow_vol
    if outflow_sink and np.any(~v):  # check condition outflow_sink and np.any(~v):
        drop = idx[~v]  # set drop
        outflow_vol = float(particles.vol[drop].sum())  # set outflow_vol
        keep = np.ones(particles.r.shape[0], dtype=bool)  # set keep
        keep[drop] = False  # execute statement
        particles = Particles(  # set particles
            r=particles.r[keep],  # set r
            c=particles.c[keep],  # set c
            vol=particles.vol[keep],  # set vol
            tau=particles.tau[keep],  # set tau
        )  # execute statement

    return particles, outflow_vol, nhops  # return particles, outflow_vol, nhops


def local_volgrid_from_particles_slab(p: Particles, r0: int, r1: int, ncols: int) -> np.ndarray:  # define function local_volgrid_from_particles_slab
    """Accumulate particle volume into a local slab grid."""  # execute statement
    slab_h = r1 - r0  # set slab_h
    volgrid = np.zeros((slab_h, ncols), dtype=np.float64)  # set volgrid
    if p.r.size == 0:  # check condition p.r.size == 0:
        return volgrid  # return volgrid
    m = (p.r >= r0) & (p.r < r1)  # set m
    if not np.any(m):  # check condition not np.any(m):
        return volgrid  # return volgrid
    rr = p.r[m] - r0  # set rr
    cc = p.c[m]  # set cc
    np.add.at(volgrid, (rr, cc), p.vol[m])  # execute statement
    return volgrid  # return volgrid
