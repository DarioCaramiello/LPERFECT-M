# -*- coding: utf-8 -*-
"""Optional array backend helpers (CPU/GPU)."""  # execute statement

# NOTE: Rain NetCDF inputs follow cdl/rain_time_dependent.cdl (CF-1.10).

# Import importlib for optional dependency checks.
import importlib  # import importlib
import importlib.util  # import importlib.util

# Import typing primitives.
from typing import Any, Optional  # import typing import Any, Optional

# Import numpy.
import numpy as np  # import numpy as np

_CUPY_SPEC = importlib.util.find_spec("cupy")  # set _CUPY_SPEC
if _CUPY_SPEC is not None:  # check condition _CUPY_SPEC is not None:
    cupy = importlib.import_module("cupy")  # set cupy
else:  # fallback branch
    cupy = None  # set cupy


def gpu_available() -> bool:  # define function gpu_available
    """Return True if CuPy is available for GPU execution."""  # execute statement
    return cupy is not None  # return cupy is not None


def normalize_device(device: Optional[str]) -> str:  # define function normalize_device
    """Normalize the device string to 'cpu' or 'gpu'."""  # execute statement
    if device is None:  # check condition device is None:
        return "cpu"  # return "cpu"
    dev = str(device).lower().strip()  # set dev
    if dev not in ("cpu", "gpu"):  # check condition dev not in ("cpu", "gpu"):
        raise ValueError(f"Unknown device '{device}'. Use 'cpu' or 'gpu'.")  # raise ValueError(f"Unknown device '{device}'. Use 'cpu' or 'gpu'.")
    return dev  # return dev


def get_array_module(device: Optional[str]) -> Any:  # define function get_array_module
    """Return the array module (numpy or cupy) for the requested device."""  # execute statement
    dev = normalize_device(device)  # set dev
    if dev == "gpu" and cupy is not None:  # check condition dev == "gpu" and cupy is not None:
        return cupy  # return cupy
    return np  # return np


def to_device(arr: Any, xp: Any) -> Any:  # define function to_device
    """Move array-like data to the selected backend."""  # execute statement
    if xp is np:  # check condition xp is np:
        return np.asarray(arr)  # return np.asarray(arr)
    return xp.asarray(arr)  # return xp.asarray(arr)


def to_numpy(arr: Any) -> Any:  # define function to_numpy
    """Move backend arrays to NumPy."""  # execute statement
    if cupy is not None and isinstance(arr, cupy.ndarray):  # check condition cupy is not None and isinstance(arr, cupy.ndarray):
        return cupy.asnumpy(arr)  # return cupy.asnumpy(arr)
    return arr  # return arr
