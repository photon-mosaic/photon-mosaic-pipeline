"""
Snakemake utility functions for photon-mosaic.

This module provides utility functions used in Snakemake workflows,
including path handling, GPU/CUDA checks, and workflow configuration.
"""

import logging
import os
from importlib.resources import files


def get_snakefile_path():
    """
    Get the path to the Snakemake workflow file.

    Returns
    -------
    Path
        Path to the Snakefile in the photon_mosaic package.
    """
    return files("photon_mosaic").joinpath("workflow", "Snakefile")


def cross_platform_path(path):
    """
    Convert path to string format appropriate for the current platform.

    On Windows, uses forward slashes (as_posix()) for Snakemake compatibility.
    On Unix-like systems, uses native path separators (str()).

    Parameters
    ----------
    path : Path
        Path object to convert

    Returns
    -------
    str
        String representation of the path appropriate for the platform
    """
    if os.name == "nt":
        return path.as_posix()
    else:
        return str(path)


def log_cuda_availability():
    """Log CUDA availability as a sanity check for GPU jobs.

    This function checks if CUDA is available and logs relevant
    GPU information. Useful for verifying that GPU resources
    are properly allocated in SLURM jobs.

    Returns
    -------
    bool
        True if CUDA is available, False otherwise
    """
    logger = logging.getLogger(__name__)

    try:
        import torch

        #  log torch version
        logger.info(f"  PyTorch version: {torch.__version__}")

        cuda_available = torch.cuda.is_available()
        logger.info(f"CUDA sanity check - Available: {cuda_available}")

        if cuda_available:
            logger.info(f"  Device count: {torch.cuda.device_count()}")
            logger.info(f"  Device name: {torch.cuda.get_device_name(0)}")
            logger.info(f"  CUDA module loaded: {torch.version.cuda}")
        else:
            logger.warning("CUDA not available - jobs may not use GPU!")

        return cuda_available
    except ImportError:
        logger.warning("PyTorch not available - cannot check CUDA status")
        return False
