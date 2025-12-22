# -*- coding: utf-8 -*-
"""D8 routing utilities."""  # execute statement

# NOTE: Rain NetCDF inputs follow cdl/rain_time_dependent.cdl (CF-1.10).

# Import typing primitives.
from typing import Dict, Tuple  # import typing import Dict, Tuple

# Import numpy for array computations.
import numpy as np  # import numpy as np

# ESRI D8 code mapping: 1=E,2=SE,4=S,8=SW,16=W,32=NW,64=N,128=NE.
ESRI_OFFSETS: Dict[int, Tuple[int, int]] = {  # execute statement
    1: (0, 1),  # execute statement
    2: (1, 1),  # execute statement
    4: (1, 0),  # execute statement
    8: (1, -1),  # execute statement
    16: (0, -1),  # execute statement
    32: (-1, -1),  # execute statement
    64: (-1, 0),  # execute statement
    128: (-1, 1),  # execute statement
}  # execute statement

# Clockwise encoding (0..7): 0=E,1=SE,2=S,3=SW,4=W,5=NW,6=N,7=NE.
CW0_7_OFFSETS: Dict[int, Tuple[int, int]] = {  # execute statement
    0: (0, 1),  # execute statement
    1: (1, 1),  # execute statement
    2: (1, 0),  # execute statement
    3: (1, -1),  # execute statement
    4: (0, -1),  # execute statement
    5: (-1, -1),  # execute statement
    6: (-1, 0),  # execute statement
    7: (-1, 1),  # execute statement
}  # execute statement


def build_downstream_index(d8: np.ndarray, encoding: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:  # define function build_downstream_index
    """Build downstream indices for each cell from a D8 grid.

    Returns
    -------
    valid : 2D bool
        True where downstream neighbor exists and is inside the grid.
    ds_r, ds_c : 2D int32
        Downstream row/col indices (or -1 where invalid).
    """
    # Normalize encoding string.
    enc = encoding.lower().strip()  # set enc
    # Choose mapping table.
    if enc == "esri":  # check condition enc == "esri":
        offsets = ESRI_OFFSETS  # set offsets
    elif enc in ("cw0_7", "clockwise0_7", "0_7"):  # check alternate condition enc in ("cw0_7", "clockwise0_7", "0_7"):
        offsets = CW0_7_OFFSETS  # set offsets
    else:  # fallback branch
        # Fail early if encoding is unknown.
        raise ValueError(f"Unknown D8 encoding '{encoding}'. Use 'esri' or 'cw0_7'.")  # raise ValueError(f"Unknown D8 encoding '{encoding}'. Use 'esri' or 'cw0_7'.")

    # Grid size.
    nrows, ncols = d8.shape  # set nrows, ncols
    # Allocate outputs with invalid defaults.
    ds_r = np.full((nrows, ncols), -1, dtype=np.int32)  # set ds_r
    ds_c = np.full((nrows, ncols), -1, dtype=np.int32)  # set ds_c
    valid = np.zeros((nrows, ncols), dtype=bool)  # set valid

    # Apply each code mapping.
    for code, (dr, dc) in offsets.items():  # loop over code, (dr, dc) in offsets.items():
        # Cells with this code.
        mask = (d8 == code)  # set mask
        # Skip if absent.
        if not np.any(mask):  # check condition not np.any(mask):
            continue  # continue loop
        # Coordinates of matching cells.
        rr, cc = np.nonzero(mask)  # set rr, cc
        # Downstream coordinates.
        r2 = rr + dr  # set r2
        c2 = cc + dc  # set c2
        # Keep only those that stay in grid bounds.
        inside = (r2 >= 0) & (r2 < nrows) & (c2 >= 0) & (c2 < ncols)  # set inside
        rr, cc, r2, c2 = rr[inside], cc[inside], r2[inside], c2[inside]  # set rr, cc, r2, c2
        # Store downstream indices.
        ds_r[rr, cc] = r2.astype(np.int32)  # execute statement
        ds_c[rr, cc] = c2.astype(np.int32)  # execute statement
        # Mark valid.
        valid[rr, cc] = True  # execute statement

    # Return.
    return valid, ds_r, ds_c  # return valid, ds_r, ds_c
