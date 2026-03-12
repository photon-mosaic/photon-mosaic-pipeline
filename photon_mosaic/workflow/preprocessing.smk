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
from photon_mosaic.rules.preprocessing import run_preprocessing
from photon_mosaic.snakemake_utils import cross_platform_path
import re
import logging
import os

# Configure SLURM resources if enabled
slurm_config = config.get("slurm", {}) if config.get("use_slurm") else {}


# Preprocessing rule
rule preprocessing:
    input:
        img=lambda wildcards: cross_platform_path(
            raw_data_base
            / discoverer.original_datasets[
                discoverer.transformed_datasets.index(wildcards.subject_name)
            ]
            / discoverer.get_tiff_relative_path_for_subject_session_file(
                wildcards.subject_name,
                discoverer.extract_session_idx_from_session_name(
                    wildcards.session_name
                ),
                wildcards.tiff,
            )
        ),
    output:
        processed=cross_platform_path(
            Path(processed_data_base).resolve()
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / (f"{output_pattern}" + "{tiff}")
        ),
    params:
        dataset_folder=lambda wildcards: cross_platform_path(
            raw_data_base
            / discoverer.original_datasets[
                discoverer.transformed_datasets.index(wildcards.subject_name)
            ]
        ),
        output_folder=lambda wildcards: cross_platform_path(
            Path(processed_data_base).resolve()
            / wildcards.subject_name
            / wildcards.session_name
            / "funcimg"
        ),
        ses_idx=lambda wildcards: discoverer.extract_session_idx_from_session_name(
            wildcards.session_name
        ),
    wildcard_constraints:
        tiff=("|".join(sorted([Path(f).name for f in discoverer.tiff_files_flat]))),
        subject_name="|".join(discoverer.transformed_datasets),
        session_name="|".join(
            [
                discoverer.get_session_name(i, session_idx)
                for i in range(len(discoverer.transformed_datasets))
                for session_idx in discoverer.tiff_files[
                    discoverer.original_datasets[i]
                ].keys()
            ]
        ),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic.rules.preprocessing import run_preprocessing
        from photon_mosaic import log_cuda_availability

        # Check CUDA availability for this job
        log_cuda_availability()

        run_preprocessing(
            Path(params.output_folder),
            config["preprocessing"],
            Path(params.dataset_folder),
            ses_idx=int(params.ses_idx),
            tiff_name=wildcards.tiff,
        )
