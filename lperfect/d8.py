# -*- coding: utf-8 -*-
"""D8 routing utilities."""

# Import typing primitives.
from typing import Dict, Tuple

# Import numpy for array computations.
import numpy as np

# ESRI D8 code mapping: 1=E,2=SE,4=S,8=SW,16=W,32=NW,64=N,128=NE.
ESRI_OFFSETS: Dict[int, Tuple[int, int]] = {
    1: (0, 1),
    2: (1, 1),
    4: (1, 0),
    8: (1, -1),
    16: (0, -1),
    32: (-1, -1),
    64: (-1, 0),
    128: (-1, 1),
}

# Clockwise encoding (0..7): 0=E,1=SE,2=S,3=SW,4=W,5=NW,6=N,7=NE.
CW0_7_OFFSETS: Dict[int, Tuple[int, int]] = {
    0: (0, 1),
    1: (1, 1),
    2: (1, 0),
    3: (1, -1),
    4: (0, -1),
    5: (-1, -1),
    6: (-1, 0),
    7: (-1, 1),
}


def build_downstream_index(d8: np.ndarray, encoding: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Build downstream indices for each cell from a D8 grid.

    Returns
    -------
    valid : 2D bool
        True where downstream neighbor exists and is inside the grid.
    ds_r, ds_c : 2D int32
        Downstream row/col indices (or -1 where invalid).
    """
    # Normalize encoding string.
    enc = encoding.lower().strip()
    # Choose mapping table.
    if enc == "esri":
        offsets = ESRI_OFFSETS
    elif enc in ("cw0_7", "clockwise0_7", "0_7"):
        offsets = CW0_7_OFFSETS
    else:
        # Fail early if encoding is unknown.
        raise ValueError(f"Unknown D8 encoding '{encoding}'. Use 'esri' or 'cw0_7'.")

    # Grid size.
    nrows, ncols = d8.shape
    # Allocate outputs with invalid defaults.
    ds_r = np.full((nrows, ncols), -1, dtype=np.int32)
    ds_c = np.full((nrows, ncols), -1, dtype=np.int32)
    valid = np.zeros((nrows, ncols), dtype=bool)

    # Apply each code mapping.
    for code, (dr, dc) in offsets.items():
        # Cells with this code.
        mask = (d8 == code)
        # Skip if absent.
        if not np.any(mask):
            continue
        # Coordinates of matching cells.
        rr, cc = np.nonzero(mask)
        # Downstream coordinates.
        r2 = rr + dr
        c2 = cc + dc
        # Keep only those that stay in grid bounds.
        inside = (r2 >= 0) & (r2 < nrows) & (c2 >= 0) & (c2 < ncols)
        rr, cc, r2, c2 = rr[inside], cc[inside], r2[inside], c2[inside]
        # Store downstream indices.
        ds_r[rr, cc] = r2.astype(np.int32)
        ds_c[rr, cc] = c2.astype(np.int32)
        # Mark valid.
        valid[rr, cc] = True

    # Return.
    return valid, ds_r, ds_c
