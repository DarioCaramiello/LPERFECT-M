# -*- coding: utf-8 -*-
"""Hydrogeological risk index computation."""

# Import numpy.
import numpy as np

# Import local D8 utility to build downstream indices.
from .d8 import build_downstream_index


def compute_flow_accum_area_m2(d8: np.ndarray, encoding: str, cell_area_m2: float | np.ndarray, active_mask: np.ndarray) -> np.ndarray:
    """Compute upstream contributing area (m^2) via topological traversal."""
    valid, ds_r, ds_c = build_downstream_index(d8, encoding)
    H, W = d8.shape

    if np.isscalar(cell_area_m2):
        acc = np.where(active_mask, float(cell_area_m2), 0.0).astype(np.float64)
    else:
        acc = np.where(active_mask, cell_area_m2, 0.0).astype(np.float64)

    indeg = np.zeros((H, W), dtype=np.int32)

    rr, cc = np.nonzero(active_mask & valid)
    rds = ds_r[rr, cc]
    cds = ds_c[rr, cc]
    np.add.at(indeg, (rds, cds), 1)

    q_r, q_c = np.nonzero(active_mask & (indeg == 0))
    stack_r = q_r.astype(np.int32).tolist()
    stack_c = q_c.astype(np.int32).tolist()

    while stack_r:
        r = stack_r.pop()
        c = stack_c.pop()
        if not (active_mask[r, c] and valid[r, c]):
            continue
        rd = int(ds_r[r, c])
        cd = int(ds_c[r, c])
        if rd < 0 or cd < 0:
            continue
        acc[rd, cd] += acc[r, c]
        indeg[rd, cd] -= 1
        if indeg[rd, cd] == 0 and active_mask[rd, cd]:
            stack_r.append(rd)
            stack_c.append(cd)

    return acc


def robust_normalize(a: np.ndarray, mask: np.ndarray, p_low: float, p_high: float) -> np.ndarray:
    """Robust normalization to [0,1] using percentiles."""
    x = np.where(mask, a, np.nan).astype(np.float64)
    lo = np.nanpercentile(x, p_low)
    hi = np.nanpercentile(x, p_high)
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return np.where(mask, 0.0, np.nan)
    y = (x - lo) / (hi - lo)
    return np.where(mask, np.clip(y, 0.0, 1.0), np.nan)


def compute_risk_index(runoff_cum_mm: np.ndarray, flow_accum_m2: np.ndarray, active_mask: np.ndarray,
                       balance: float, p_low: float, p_high: float) -> np.ndarray:
    """Combine normalized runoff and flow accumulation into a unitless risk index."""
    alpha = float(np.clip(balance, 0.0, 1.0))
    r1 = robust_normalize(runoff_cum_mm, active_mask, p_low, p_high)
    r2 = robust_normalize(flow_accum_m2, active_mask, p_low, p_high)
    return np.where(active_mask, alpha * r1 + (1.0 - alpha) * r2, np.nan)
