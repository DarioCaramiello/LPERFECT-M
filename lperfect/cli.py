# -*- coding: utf-8 -*-
"""Command line interface for LPERFECT."""  # execute statement

# Import argparse for CLI parsing.
import argparse  # import argparse


def parse_args() -> argparse.Namespace:  # define function parse_args
    """Parse command-line arguments."""  # execute statement
    # Create argument parser with a program name.
    ap = argparse.ArgumentParser(prog="lperfect")  # set ap
    # Configuration file path.
    ap.add_argument("--config", default="config.json", help="Path to configuration JSON file.")  # execute statement
    # Logging level.
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])  # execute statement
    # Common operational overrides for pipelines.
    ap.add_argument("--restart-in", default=None, help="Restart NetCDF to resume from.")  # execute statement
    ap.add_argument("--restart-out", default=None, help="Restart NetCDF path to write.")  # execute statement
    ap.add_argument("--out-nc", default=None, help="Output NetCDF path (rank 0 only).")  # execute statement
    ap.add_argument("--device", default=None, choices=["cpu", "gpu"], help="Compute device override.")  # execute statement
    # Return parsed args.
    return ap.parse_args()  # return ap.parse_args()
