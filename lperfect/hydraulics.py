# -*- coding: utf-8 -*-
"""Lagrangian routing helpers: particle spawning and advection."""

# Import typing primitives.
from typing import Optional, Tuple

# Import numpy.
import numpy as np

# Import local particle structures.
from .particles import Particles, empty_particles


def cell_area_at(area: float | np.ndarray, rr: np.ndarray, cc: np.ndarray) -> np.ndarray:
    """Return cell area(s) for indices rr,cc."""
    if np.isscalar(area):
        return np.full(rr.shape, float(area), dtype=np.float64)
    return area[rr, cc].astype(np.float64)


def spawn_particles_from_runoff_slab(
    runoff_depth_m_slab: np.ndarray,
    r0: int,
    cell_area_m2: float | np.ndarray,
    particle_vol_m3: float,
    active_mask_global: np.ndarray,
) -> Tuple[Particles, float]:
    """Convert incremental runoff depth (m) into particles for a local slab."""
    depth = np.maximum(runoff_depth_m_slab, 0.0)
    rr_local, cc = np.nonzero(depth > 0.0)
    if rr_local.size == 0:
        return empty_particles(), 0.0

    rr_global = rr_local + r0

    ok = active_mask_global[rr_global, cc]
    rr_local = rr_local[ok]
    rr_global = rr_global[ok]
    cc = cc[ok]
    if rr_global.size == 0:
        return empty_particles(), 0.0

    area = cell_area_at(cell_area_m2, rr_global, cc)
    vols = depth[rr_local, cc] * area
    total_vol = float(vols.sum())

    n = np.maximum(1, np.round(vols / particle_vol_m3).astype(np.int32))

    r = np.repeat(rr_global.astype(np.int32), n)
    c = np.repeat(cc.astype(np.int32), n)
    vol = np.repeat((vols / n).astype(np.float64), n)
    tau = np.zeros_like(vol, dtype=np.float64)

    return Particles(r=r, c=c, vol=vol, tau=tau), total_vol


def advect_particles_one_step(
    particles: Particles,
    valid: np.ndarray,
    ds_r: np.ndarray,
    ds_c: np.ndarray,
    dt_s: float,
    travel_time_s: float,
    travel_time_channel_s: float,
    channel_mask: Optional[np.ndarray],
    outflow_sink: bool,
) -> Tuple[Particles, float, int]:
    """Advance particles one step, with travel-time gating."""
    if particles.r.size == 0:
        return particles, 0.0, 0

    particles.tau = particles.tau - dt_s

    can_move = (particles.tau <= 0.0)
    if not np.any(can_move):
        return particles, 0.0, 0

    idx = np.nonzero(can_move)[0]
    r0 = particles.r[idx]
    c0 = particles.c[idx]

    v = valid[r0, c0]
    nhops = int(np.count_nonzero(v))

    if np.any(v):
        rds = ds_r[r0[v], c0[v]]
        cds = ds_c[r0[v], c0[v]]
        moved = idx[v]

        particles.r[moved] = rds
        particles.c[moved] = cds

        if channel_mask is not None:
            is_ch = channel_mask[rds, cds]
            particles.tau[moved] = particles.tau[moved] + np.where(is_ch, travel_time_channel_s, travel_time_s)
        else:
            particles.tau[moved] = particles.tau[moved] + travel_time_s

    outflow_vol = 0.0
    if outflow_sink and np.any(~v):
        drop = idx[~v]
        outflow_vol = float(particles.vol[drop].sum())
        keep = np.ones(particles.r.shape[0], dtype=bool)
        keep[drop] = False
        particles = Particles(
            r=particles.r[keep],
            c=particles.c[keep],
            vol=particles.vol[keep],
            tau=particles.tau[keep],
        )

    return particles, outflow_vol, nhops


def local_volgrid_from_particles_slab(p: Particles, r0: int, r1: int, ncols: int) -> np.ndarray:
    """Accumulate particle volume into a local slab grid."""
    slab_h = r1 - r0
    volgrid = np.zeros((slab_h, ncols), dtype=np.float64)
    if p.r.size == 0:
        return volgrid
    m = (p.r >= r0) & (p.r < r1)
    if not np.any(m):
        return volgrid
    rr = p.r[m] - r0
    cc = p.c[m]
    np.add.at(volgrid, (rr, cc), p.vol[m])
    return volgrid
