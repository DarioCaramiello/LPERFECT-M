# -*- coding: utf-8 -*-
"""Particle container and helpers."""

# Import dataclass for a simple structured object.
from dataclasses import dataclass

# Import numpy for arrays.
import numpy as np


@dataclass
class Particles:
    """Structure-of-arrays particle container.

    Attributes
    ----------
    r, c : np.ndarray (int32)
        Row and column indices of each particle.
    vol : np.ndarray (float64)
        Particle volume in cubic meters.
    tau : np.ndarray (float64)
        Time-to-next-hop in seconds (<=0 means eligible to move).
    """
    r: np.ndarray
    c: np.ndarray
    vol: np.ndarray
    tau: np.ndarray


def empty_particles() -> Particles:
    """Create an empty particle container."""
    return Particles(
        r=np.zeros(0, dtype=np.int32),
        c=np.zeros(0, dtype=np.int32),
        vol=np.zeros(0, dtype=np.float64),
        tau=np.zeros(0, dtype=np.float64),
    )


def concat_particles(a: Particles, b: Particles) -> Particles:
    """Concatenate two particle containers."""
    # If one is empty, return the other.
    if a.r.size == 0:
        return b
    if b.r.size == 0:
        return a
    # Concatenate fields.
    return Particles(
        r=np.concatenate([a.r, b.r]),
        c=np.concatenate([a.c, b.c]),
        vol=np.concatenate([a.vol, b.vol]),
        tau=np.concatenate([a.tau, b.tau]),
    )


def pack_particles_to_float64(p: Particles) -> np.ndarray:
    """Pack particles into float64 matrix (N,4) for MPI transfer."""
    # Allocate (N,4).
    buf = np.empty((p.r.size, 4), dtype=np.float64)
    # Copy fields to columns (cast ints to float for a single MPI datatype).
    buf[:, 0] = p.r.astype(np.float64)
    buf[:, 1] = p.c.astype(np.float64)
    buf[:, 2] = p.vol.astype(np.float64)
    buf[:, 3] = p.tau.astype(np.float64)
    # Return packed matrix.
    return buf


def unpack_particles_from_float64(buf: np.ndarray) -> Particles:
    """Unpack float64 matrix (N,4) back to Particles."""
    # Handle empty case.
    if buf.size == 0:
        return empty_particles()
    # Build Particles with proper dtypes.
    return Particles(
        r=buf[:, 0].astype(np.int32),
        c=buf[:, 1].astype(np.int32),
        vol=buf[:, 2].astype(np.float64),
        tau=buf[:, 3].astype(np.float64),
    )
