"""
Suite2p Analysis Module

This Snakefile module handles the Suite2p analysis step of the photon mosaic pipeline.
It takes preprocessed TIFF files and runs Suite2p to extract neural activity traces.

The suite2p rule:
- Takes preprocessed TIFF files as input (from preprocessing step)
- Runs Suite2p analysis with parameters from config["suite2p_ops"]
- Outputs F.npy (fluorescence traces) and data.bin (binary data) files
- Supports SLURM cluster execution with configurable resources

Input: Preprocessed TIFF files from the preprocessing step
Output: Suite2p analysis results (F.npy, data.bin) in suite2p/plane0/ directory
"""

import re
from photon_mosaic.snakemake_utils import cross_platform_path


rule suite2p:
    input:
        tiffs=lambda wildcards: [
            cross_platform_path(
                Path(processed_data_base).resolve()
                / wildcards.subject_name
                / wildcards.session_name
                / "funcimg"
                / f"{output_pattern}{Path(tiff_name).name}"
            )
            for tiff_name in discoverer.tiff_files[
                discoverer.original_datasets[
                    discoverer.transformed_datasets.index(wildcards.subject_name)
                ]
            ][discoverer.extract_session_idx_from_session_name(wildcards.session_name)]
        ],
    output:
        F=cross_platform_path(
            Path(processed_data_base).resolve()
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p"
            / "plane0"
            / "F.npy"
        ),
        bin=cross_platform_path(
            Path(processed_data_base).resolve()
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p"
            / "plane0"
            / "data.bin"
        ),
    params:
        dataset_folder=lambda wildcards: cross_platform_path(
            Path(processed_data_base).resolve()
            / wildcards.subject_name
            / wildcards.session_name
            / "funcimg"
        ),
    wildcard_constraints:
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
        from photon_mosaic.rules.suite2p_run import run_suite2p
        from photon_mosaic import log_cuda_availability
        from pathlib import Path

        # Check CUDA availability for this job
        log_cuda_availability()

        # Ensure all paths are properly resolved
        input_paths = [Path(tiff).resolve() for tiff in input.tiffs]
        output_path = Path(output.F).resolve()
        dataset_folder = Path(params.dataset_folder).resolve()

        run_suite2p(
            str(output_path),
            dataset_folder,
            config["suite2p_ops"],
        )
