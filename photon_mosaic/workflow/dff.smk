"""
dF/F Calculation Module

This Snakefile module handles the dF/F calculation step of the photon mosaic pipeline.
It takes Suite2p outputs and compute dF/F.

The suite2p rule:
- Takes preprocessed TIFF files as input (from preprocessing step)
- Runs Suite2p analysis with parameters from config["suite2p_ops"]
- Outputs F.npy (fluorescence traces) and data.bin (binary data) files
- Supports SLURM cluster execution with configurable resources

Input: Suite2p analysis results (F.npy, Fneu.npy) in suite2p/plane0/ directory
Output: dff calculation results (dFF.npy) in dff/plane0/ directory
"""

import re
from photon_mosaic.snakemake_utils import cross_platform_path


rule dff:
    input:
        F=cross_platform_path(
            Path(processed_data_base).resolve()
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "suite2p"
            / "plane0"
            / "F.npy"
        ),
    output:
        dFF=cross_platform_path(
            Path(processed_data_base).resolve()
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "dff"
            / "plane0"
            / "dFF.npy"
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
        from photon_mosaic.rules.dff_run import calculate_dFF
        from pathlib import Path

        # Ensure all paths are properly resolved
        input_path_F = Path(input.F).resolve()
        input_path_Fneu = Path(input.F).parent.resolve() / "Fneu.npy"
        output_path = Path(output.dFF).resolve()

        calculate_dFF(
            str(input_path_F),
            str(input_path_Fneu),
            str(output_path),
            config["dff_ops"],
        )
