# -*- coding: utf-8 -*-
"""LPERFECT package.

This package contains the modular implementation of the LPERFECT model:
Lagrangian Parallel Environmental Runoff and Flood Evaluation for Computational Terrain.

The top-level executable is `main.py` (in the project root).
"""

# NOTE: Rain NetCDF inputs follow cdl/rain_time_dependent.cdl (CF-1.10).

# Expose a version string for provenance (optional, but convenient).
__version__ = "1.0.0"  # set __version__
RAIN_SCHEMA_DOC = "cdl/rain_time_dependent.cdl"  # execute statement

# Convenience exports for callers that import from the package root.
from .simulation import run_simulation  # noqa: E402

__all__ = ["run_simulation", "__version__", "RAIN_SCHEMA_DOC"]
