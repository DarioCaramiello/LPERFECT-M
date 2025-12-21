# -*- coding: utf-8 -*-
"""MPI utilities for LPERFECT.

This module provides:
- MPI initialization (optional)
- slab decomposition helpers
- particle migration (Alltoallv)
- scatter/gather helpers for restart and output
"""

# Import typing primitives.
from typing import Any, List, Optional, Tuple

# Import numpy for counts/displacements arrays.
import numpy as np

# Import local particle helpers.
from .particles import Particles, empty_particles, pack_particles_to_float64, unpack_particles_from_float64


# Try importing mpi4py; allow serial fallback.
try:
    from mpi4py import MPI  # type: ignore
    HAVE_MPI = True
except Exception:
    MPI = None  # type: ignore
    HAVE_MPI = False


def get_comm() -> tuple[Any, int, int]:
    """Return (comm, rank, size) for MPI or serial."""
    # If mpi4py is available, use COMM_WORLD.
    if HAVE_MPI:
        comm = MPI.COMM_WORLD
        return comm, comm.Get_rank(), comm.Get_size()
    # Otherwise emulate a single-rank communicator as (None,0,1).
    return None, 0, 1


def slab_counts_starts(nrows: int, size: int) -> tuple[np.ndarray, np.ndarray]:
    """Compute slab row counts and starts for each rank."""
    # Start with floor division.
    counts = np.full(size, nrows // size, dtype=np.int32)
    # Distribute remainder to the first ranks.
    counts[: (nrows % size)] += 1
    # Compute starts as prefix sums of counts.
    starts = np.zeros(size, dtype=np.int32)
    starts[1:] = np.cumsum(counts[:-1])
    return counts, starts


def slab_bounds(nrows: int, size: int, rank: int) -> tuple[int, int]:
    """Return (r0,r1) bounds of the slab owned by `rank`."""
    # Get counts and starts.
    counts, starts = slab_counts_starts(nrows, size)
    # Start row.
    r0 = int(starts[rank])
    # End row (exclusive).
    r1 = int(starts[rank] + counts[rank])
    return r0, r1


def rank_of_row(r: np.ndarray, nrows: int, size: int) -> np.ndarray:
    """Map each row index in r to its owning rank."""
    # Build ends array (start+count for each rank).
    counts, starts = slab_counts_starts(nrows, size)
    ends = starts + counts
    # searchsorted finds the first end > r.
    return np.searchsorted(ends, r, side="right").astype(np.int32)


def alltoallv_float64(comm, sendbuf_by_rank: List[np.ndarray]) -> np.ndarray:
    """Alltoallv exchange for variable-sized float64 buffers."""
    # Number of ranks.
    size = comm.Get_size()
    # Build sendcounts.
    sendcounts = np.array([b.size for b in sendbuf_by_rank], dtype=np.int64)
    # Build send displacements.
    senddispls = np.zeros(size, dtype=np.int64)
    senddispls[1:] = np.cumsum(sendcounts[:-1])
    # Flatten outgoing buffers.
    sendflat = np.concatenate(sendbuf_by_rank).astype(np.float64) if sendcounts.sum() else np.zeros(0, dtype=np.float64)
    # Receive counts from other ranks.
    recvcounts = np.zeros(size, dtype=np.int64)
    comm.Alltoall(sendcounts, recvcounts)
    # Build receive displacements.
    recvdispls = np.zeros(size, dtype=np.int64)
    recvdispls[1:] = np.cumsum(recvcounts[:-1])
    # Allocate receive buffer.
    recvflat = np.empty(int(recvcounts.sum()), dtype=np.float64)
    # Perform exchange.
    comm.Alltoallv(
        [sendflat, sendcounts, senddispls, MPI.DOUBLE],
        [recvflat, recvcounts, recvdispls, MPI.DOUBLE],
    )
    # Return received flat buffer.
    return recvflat


def migrate_particles_slab(comm, particles: Particles, nrows: int) -> Particles:
    """Migrate particles between ranks based on slab ownership."""
    # Rank and size.
    rank = comm.Get_rank()
    size = comm.Get_size()
    # Destination rank for each particle.
    dest = rank_of_row(particles.r, nrows, size)
    # Keep those that remain local.
    keep_mask = (dest == rank)
    local = Particles(
        r=particles.r[keep_mask],
        c=particles.c[keep_mask],
        vol=particles.vol[keep_mask],
        tau=particles.tau[keep_mask],
    )
    # Build per-destination send buffers.
    sendbuf_by_rank: List[np.ndarray] = []
    for dst in range(size):
        # Skip self-destination.
        if dst == rank:
            sendbuf_by_rank.append(np.zeros(0, dtype=np.float64))
            continue
        # Mask for particles going to dst.
        m = (dest == dst)
        if np.any(m):
            # Pack to float64 and flatten.
            buf = pack_particles_to_float64(Particles(
                r=particles.r[m], c=particles.c[m], vol=particles.vol[m], tau=particles.tau[m]
            )).ravel()
            sendbuf_by_rank.append(buf)
        else:
            sendbuf_by_rank.append(np.zeros(0, dtype=np.float64))
    # Exchange buffers.
    recvflat = alltoallv_float64(comm, sendbuf_by_rank)
    # If nothing received, return local only.
    if recvflat.size == 0:
        return local
    # Sanity check: payload must be multiple of 4 floats.
    if recvflat.size % 4 != 0:
        raise RuntimeError("Received particle payload is not divisible by 4")
    # Reshape and unpack.
    received = unpack_particles_from_float64(recvflat.reshape((-1, 4)))
    # Concatenate local and received.
    from .particles import concat_particles
    return concat_particles(local, received)


def scatter_field_slab(comm, full: Optional[np.ndarray], nrows: int, ncols: int, dtype) -> np.ndarray:
    """Scatter full (nrows,ncols) 2D array from rank0 to slabs on all ranks."""
    # Rank/size.
    rank = comm.Get_rank()
    size = comm.Get_size()
    # Slab counts/starts.
    counts, starts = slab_counts_starts(nrows, size)
    # Local bounds.
    r0, r1 = slab_bounds(nrows, size, rank)
    slab_h = r1 - r0
    # Element counts/displacements for the flattened arrays.
    sendcounts = (counts.astype(np.int64) * ncols).astype(np.int64)
    displs = (starts.astype(np.int64) * ncols).astype(np.int64)
    # Allocate local slab.
    local = np.empty((slab_h, ncols), dtype=dtype)
    # Prepare rank0 send buffer.
    sendbuf = full.astype(dtype).ravel() if rank == 0 and full is not None else None
    # Map dtype to MPI datatype.
    mpitype = MPI._typedict[np.dtype(dtype).char]
    # Scatter.
    comm.Scatterv([sendbuf, sendcounts, displs, mpitype], local.ravel(), root=0)
    # Return local slab.
    return local


def gather_field_slab_to_rank0(comm, slab: np.ndarray, nrows: int, ncols: int) -> Optional[np.ndarray]:
    """Gather slabs from all ranks to a full array on rank0."""
    # Rank/size.
    rank = comm.Get_rank()
    size = comm.Get_size()
    # Slab counts/starts.
    counts, starts = slab_counts_starts(nrows, size)
    # Receive counts/displacements.
    recvcounts = (counts.astype(np.int64) * ncols).astype(np.int64)
    displs = (starts.astype(np.int64) * ncols).astype(np.int64)
    # Allocate on rank0 only.
    full_flat = np.empty((nrows * ncols,), dtype=slab.dtype) if rank == 0 else None
    # MPI datatype.
    mpitype = MPI._typedict[np.dtype(slab.dtype).char]
    # Gather.
    comm.Gatherv(slab.ravel(), [full_flat, recvcounts, displs, mpitype], root=0)
    # Return full array on rank0.
    if rank != 0:
        return None
    return full_flat.reshape((nrows, ncols))


def gather_particles_to_rank0(comm, p_local: Particles) -> Particles:
    """Gather particles from all ranks to rank0 (using Alltoallv pattern)."""
    # Rank/size.
    rank = comm.Get_rank()
    size = comm.Get_size()
    # Pack local particles.
    buf = pack_particles_to_float64(p_local).ravel()
    # Build send list: only destination rank 0 gets the payload.
    sendbuf_by_rank = [buf if dst == 0 else np.zeros(0, dtype=np.float64) for dst in range(size)]
    # Exchange.
    recvflat = alltoallv_float64(comm, sendbuf_by_rank)
    # Non-root returns empty.
    if rank != 0 or recvflat.size == 0:
        return empty_particles()
    # Unpack on root.
    return unpack_particles_from_float64(recvflat.reshape((-1, 4)))


def scatter_particles_from_rank0(comm, p_all: Optional[Particles], nrows: int) -> Particles:
    """Scatter particles from rank0 to owning ranks based on row ownership."""
    # Rank/size.
    rank = comm.Get_rank()
    size = comm.Get_size()
    # Prepare send buffers.
    if rank == 0 and p_all is not None and p_all.r.size > 0:
        # Compute destination for each particle.
        dest = rank_of_row(p_all.r, nrows, size)
        sendbuf_by_rank: List[np.ndarray] = []
        # Build payload per destination.
        for dst in range(size):
            m = (dest == dst)
            if np.any(m):
                sendbuf_by_rank.append(pack_particles_to_float64(Particles(
                    r=p_all.r[m], c=p_all.c[m], vol=p_all.vol[m], tau=p_all.tau[m]
                )).ravel())
            else:
                sendbuf_by_rank.append(np.zeros(0, dtype=np.float64))
    else:
        # Non-root ranks send empty buffers.
        sendbuf_by_rank = [np.zeros(0, dtype=np.float64) for _ in range(size)]
    # Exchange.
    recvflat = alltoallv_float64(comm, sendbuf_by_rank)
    # Unpack.
    if recvflat.size == 0:
        return empty_particles()
    if recvflat.size % 4 != 0:
        raise RuntimeError("Received particle payload not divisible by 4")
    return unpack_particles_from_float64(recvflat.reshape((-1, 4)))
