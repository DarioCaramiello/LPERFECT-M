# -*- coding: utf-8 -*-
"""Particle container and helpers."""  # execute statement

# Import dataclass for a simple structured object.
from dataclasses import dataclass  # import dataclasses import dataclass

# Import numpy for arrays.
import numpy as np  # import numpy as np


@dataclass  # apply decorator
class Particles:  # define class Particles
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
    r: np.ndarray  # execute statement
    c: np.ndarray  # execute statement
    vol: np.ndarray  # execute statement
    tau: np.ndarray  # execute statement


def empty_particles() -> Particles:  # define function empty_particles
    """Create an empty particle container."""  # execute statement
    return Particles(  # return Particles(
        r=np.zeros(0, dtype=np.int32),  # set r
        c=np.zeros(0, dtype=np.int32),  # set c
        vol=np.zeros(0, dtype=np.float64),  # set vol
        tau=np.zeros(0, dtype=np.float64),  # set tau
    )  # execute statement


def concat_particles(a: Particles, b: Particles) -> Particles:  # define function concat_particles
    """Concatenate two particle containers."""  # execute statement
    # If one is empty, return the other.
    if a.r.size == 0:  # check condition a.r.size == 0:
        return b  # return b
    if b.r.size == 0:  # check condition b.r.size == 0:
        return a  # return a
    # Concatenate fields.
    return Particles(  # return Particles(
        r=np.concatenate([a.r, b.r]),  # set r
        c=np.concatenate([a.c, b.c]),  # set c
        vol=np.concatenate([a.vol, b.vol]),  # set vol
        tau=np.concatenate([a.tau, b.tau]),  # set tau
    )  # execute statement


def pack_particles_to_float64(p: Particles) -> np.ndarray:  # define function pack_particles_to_float64
    """Pack particles into float64 matrix (N,4) for MPI transfer."""  # execute statement
    # Allocate (N,4).
    buf = np.empty((p.r.size, 4), dtype=np.float64)  # set buf
    # Copy fields to columns (cast ints to float for a single MPI datatype).
    buf[:, 0] = p.r.astype(np.float64)  # execute statement
    buf[:, 1] = p.c.astype(np.float64)  # execute statement
    buf[:, 2] = p.vol.astype(np.float64)  # execute statement
    buf[:, 3] = p.tau.astype(np.float64)  # execute statement
    # Return packed matrix.
    return buf  # return buf


def unpack_particles_from_float64(buf: np.ndarray) -> Particles:  # define function unpack_particles_from_float64
    """Unpack float64 matrix (N,4) back to Particles."""  # execute statement
    # Handle empty case.
    if buf.size == 0:  # check condition buf.size == 0:
        return empty_particles()  # return empty_particles()
    # Build Particles with proper dtypes.
    return Particles(  # return Particles(
        r=buf[:, 0].astype(np.int32),  # set r
        c=buf[:, 1].astype(np.int32),  # set c
        vol=buf[:, 2].astype(np.float64),  # set vol
        tau=buf[:, 3].astype(np.float64),  # set tau
    )  # execute statement
