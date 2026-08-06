"""
Neuropil Correction Module

This Snakefile module handles neuropil correction for the photon mosaic pipeline.
It takes Suite2p outputs and applies neuropil correction.

The neuropil rule:
- Takes F.npy and Fneu.npy from Suite2p as input
- Applies neuropil correction: Fc = F - neucoeff * (Fneu - median(Fneu))
- Outputs Fc.npy in neuropil/plane0/ directory
- Supports SLURM cluster execution with configurable resources

Input:  Suite2p results (F.npy, Fneu.npy) in suite2p/plane0/ directory
Output: Neuropil-corrected traces (Fc.npy) in neuropil/plane0/ directory
"""

from pathlib import Path

from photon_mosaic_pipeline.snakemake_utils import cross_platform_path
from photon_mosaic_pipeline.paths_selection import _DERIVATIVES


rule neuropil:
    input:
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
    output:
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
    wildcard_constraints:
        subject_name="|".join(_subject_names),
        session_name="|".join(_session_names),
    resources:
        **(slurm_config if config.get("use_slurm") else {}),
    run:
        from photon_mosaic_pipeline.rules.neuropil_run import (
            calculate_neuropil_correction,
        )
        from pathlib import Path

        input_path_F = Path(input.F).resolve()
        input_path_Fneu = Path(input.Fneu).resolve()
        output_path = Path(output.Fc).resolve()

        calculate_neuropil_correction(
            str(input_path_F),
            str(input_path_Fneu),
            str(output_path),
            config["neuropil_ops"],
        )
