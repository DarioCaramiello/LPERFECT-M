# -*- coding: utf-8 -*-
"""Logging utilities for LPERFECT."""

# Import the standard logging module.
import logging


def setup_logging(level: str, rank: int) -> None:
    """Configure Python logging with a rank-aware format.

    Parameters
    ----------
    level : str
        Logging level name (e.g., 'DEBUG', 'INFO').
    rank : int
        MPI rank (0 in serial).
    """
    # Convert the level string into an actual numeric level (defaults to INFO).
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    # Configure the root logger once (basicConfig is a no-op if already configured).
    logging.basicConfig(
        level=numeric_level,
        format=f"%(asctime)s [%(levelname)s] rank={rank} %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
