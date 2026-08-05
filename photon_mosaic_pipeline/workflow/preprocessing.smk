"""
Preprocessing Module
This Snakefile module handles the preprocessing step of the photon mosaic pipeline.
It processes raw TIFF files from discovered datasets and applies preprocessing
operations defined in the configuration.
The preprocessing rule:
- Takes raw TIFF files as input from the dataset discovery
- Applies preprocessing operations (defined in config["preprocessing"])
- Outputs processed files in a standardized NeuroBlueprint format
- Supports SLURM cluster execution with configurable resources
Input: Raw TIFF files from discovered datasets
Output: Preprocessed TIFF files organized by subject/session
"""

from pathlib import Path
from photon_mosaic_pipeline.rules.preprocessing import run_preprocessing
from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _RAWDATA, _DERIVATIVES
import re
import logging
import os

# Configure SLURM resources if enabled
slurm_config = config.get("slurm", {}) if config.get("use_slurm") else {}


def _get_raw_tiff_for_wildcards(wildcards):
    match = next(
        (
            p
            for p in all_selected_tiff_paths
            if p.parts[p.parts.index(_RAWDATA) + 1] == wildcards.subject_name
            and p.parts[p.parts.index(_RAWDATA) + 2] == wildcards.session_name
            and p.name == wildcards.tiff
        ),
        None,
    )
    if match is None:
        raise ValueError(
            f"No raw TIFF found for subject={wildcards.subject_name}, "
            f"session={wildcards.session_name}, tiff={wildcards.tiff}"
        )
    return cross_platform_path(match)


# Preprocessing rule
rule preprocessing:
    input:
        img=_get_raw_tiff_for_wildcards,
    output:
        processed=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / (f"{output_pattern}" + "{tiff}")
        ),
    params:
        raw_session_folder=lambda wildcards: cross_platform_path(
            project_path
            / _RAWDATA
            / wildcards.subject_name
            / wildcards.session_name
            / "funcimg"
        ),
        output_folder=lambda wildcards: cross_platform_path(
            project_path
            / _DERIVATIVES
            / wildcards.subject_name
            / wildcards.session_name
            / "funcimg"
        ),
    wildcard_constraints:
        tiff="|".join(sorted({p.name for p in all_selected_tiff_paths})),
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.preprocessing import run_preprocessing

        run_preprocessing(
            Path(params.output_folder),
            config["preprocessing"],
            Path(params.raw_session_folder),
            tiff_name=wildcards.tiff,
        )
