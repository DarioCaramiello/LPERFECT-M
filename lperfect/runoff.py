# -*- coding: utf-8 -*-
"""Runoff generation models (currently SCS Curve Number)."""

# Import numpy.
import numpy as np


def scs_cn_cumulative_runoff_mm(P_cum_mm: np.ndarray, CN: np.ndarray, ia_ratio: float) -> np.ndarray:
    """Compute cumulative runoff Q (mm) from cumulative precipitation P (mm) using SCS-CN.

    Formulae
    --------
    S  = 25400/CN - 254  [mm]
    Ia = ia_ratio * S     [mm]

    Q = 0                              if P <= Ia
    Q = (P - Ia)^2 / (P - Ia + S)       if P > Ia

    Notes
    -----
    CN must be in (0,100]. Invalid CN yields Q=0.
    """
    # Ensure float arrays.
    P = np.asarray(P_cum_mm, dtype=np.float64)
    CNv = np.asarray(CN, dtype=np.float64)

    # Initialize runoff to zero.
    Q = np.zeros_like(P, dtype=np.float64)

    # Mask valid cells.
    ok = (CNv > 0.0) & (CNv <= 100.0) & np.isfinite(CNv) & np.isfinite(P)
    if not np.any(ok):
        return Q

    # Potential retention S.
    S = np.zeros_like(P, dtype=np.float64)
    S[ok] = (25400.0 / CNv[ok]) - 254.0

    # Initial abstraction.
    Ia = ia_ratio * S

    # Condition for runoff.
    cond = ok & (P > Ia) & (S > 0.0)
    if np.any(cond):
        num = (P[cond] - Ia[cond]) ** 2
        den = (P[cond] - Ia[cond] + S[cond])
        Q[cond] = num / den

    return Q
