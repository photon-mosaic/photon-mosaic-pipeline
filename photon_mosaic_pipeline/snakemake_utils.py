"""
Snakemake utility functions for photon-mosaic-pipeline.

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
        Path to the Snakefile in the photon_mosaic_pipeline package.
    """
    return files("photon_mosaic_pipeline").joinpath("workflow", "Snakefile")


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


def slurm_resources(config, rule_name):
    """Return the SLURM resources for one rule.

    The ``slurm`` config block holds one sub-block per rule, named after the
    rule (``slurm.preprocessing``, ``slurm.suite2p``). Each rule reads its own
    block and nothing else -- there is no inheritance, so a rule never has to
    remove a resource it did not want. Keys shared between rules are best
    expressed with a YAML anchor in the config itself::

        slurm:
          _common: &common
            runtime: 120
          preprocessing:
            <<: *common
            slurm_partition: "cpu"

    A ``slurm`` block with no per-rule sub-blocks is treated as applying to
    every rule, which is how it behaved before per-rule blocks existed.

    Parameters
    ----------
    config : dict
        The full workflow config.
    rule_name : str
        Name of the rule to resolve resources for.

    Returns
    -------
    dict
        Flat ``{resource: value}`` mapping, empty when SLURM is disabled.
    """
    if not config.get("use_slurm"):
        return {}

    slurm_config = config.get("slurm") or {}
    block = slurm_config.get(rule_name)

    if block is None:
        # Backwards compatibility: a flat slurm: block (no per-rule
        # sub-blocks) applies to every rule, as it did before.
        block = {
            key: value
            for key, value in slurm_config.items()
            if not isinstance(value, dict)
        }

    # Drop nulls (not a resource) and the underscore-prefixed keys used to
    # hold YAML anchors, which are config scaffolding rather than resources.
    return {
        key: value
        for key, value in block.items()
        if value is not None and not key.startswith("_")
    }


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
