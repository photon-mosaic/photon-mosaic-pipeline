"""
dF/F Calculation Module

This Snakefile module handles the dF/F calculation step of the photon mosaic pipeline.
It takes neuropil-corrected traces and computes dF/F.

The dff rule:
- Takes Fc.npy from the neuropil correction step as input
- Computes dF/F using a Gaussian Mixture Model to estimate baseline F0
- Outputs dFF.npy and F0.npy in dff/plane0/ directory
- Supports SLURM cluster execution with configurable resources

Input:  Neuropil-corrected traces (Fc.npy) in neuropil/plane0/ directory
Output: dF/F results (dFF.npy, F0.npy) in dff/plane0/ directory
"""

import re
from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _DERIVATIVES


rule dff:
    input:
        Fc=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "neuropil"
            / "plane0"
            / "Fc.npy"
        ),
    output:
        dFF=cross_platform_path(
            project_path
            / _DERIVATIVES
            / "{subject_name}"
            / "{session_name}"
            / "funcimg"
            / "dff"
            / "plane0"
            / "dFF.npy"
        ),
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.dff_run import calculate_dFF
        from pathlib import Path

        input_path_Fc = Path(input.Fc).resolve()
        output_path = Path(output.dFF).resolve()

        calculate_dFF(
            str(input_path_Fc),
            str(output_path),
            config["dff_ops"],
        )
