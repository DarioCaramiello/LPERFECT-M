# -*- coding: utf-8 -*-
"""Logging utilities for LPERFECT."""  # execute statement

# Import the standard logging module.
import logging  # import logging


def setup_logging(level: str, rank: int) -> None:  # define function setup_logging
    """Configure Python logging with a rank-aware format.

    Parameters
    ----------
    level : str
        Logging level name (e.g., 'DEBUG', 'INFO').
    rank : int
        MPI rank (0 in serial).
    """
    # Convert the level string into an actual numeric level (defaults to INFO).
    numeric_level = getattr(logging, level.upper(), logging.INFO)  # set numeric_level
    # Configure the root logger once (basicConfig is a no-op if already configured).
    logging.basicConfig(  # execute statement
        level=numeric_level,  # set level
        format=f"%(asctime)s [%(levelname)s] rank={rank} %(name)s: %(message)s",  # set format
        datefmt="%Y-%m-%d %H:%M:%S",  # set datefmt
    )  # execute statement
