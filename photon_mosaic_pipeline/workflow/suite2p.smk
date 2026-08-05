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
from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _DERIVATIVES


rule suite2p:
    input:
        tiffs=lambda wildcards: [
            cross_platform_path(
                project_path
                / _DERIVATIVES
                / wildcards.subject_name
                / wildcards.session_name
                / "funcimg"
                / (output_pattern + p.name)
            )
            for p in all_selected_tiff_paths
            if p.parts[p.parts.index(_RAWDATA) + 1] == wildcards.subject_name
            and p.parts[p.parts.index(_RAWDATA) + 2] == wildcards.session_name
        ],
    output:
        F=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p"
            / "plane0"
            / "F.npy"
        ),
        Fneu=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p"
            / "plane0"
            / "Fneu.npy"
        ),
        bin=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p"
            / "plane0"
            / "data.bin"
        ),
    params:
        dataset_folder=lambda wildcards: cross_platform_path(
            project_path
            / _DERIVATIVES
            / wildcards.subject_name
            / wildcards.session_name
            / "funcimg"
        ),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.suite2p_run import run_suite2p
        from pathlib import Path

        # Ensure all paths are properly resolved
        output_path = Path(output.F).resolve()
        dataset_folder = Path(params.dataset_folder).resolve()
        run_suite2p(
            str(output_path),
            dataset_folder,
            config["suite2p_ops"],
        )
